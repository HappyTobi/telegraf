//go:generate ../../../tools/readme_config_includer/generator
package eventhub_consumer

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	ioFs "io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
)

//go:embed sample.conf
var sampleConfig string

var once sync.Once

const (
	defaultMaxUndeliveredMessages = 1000
	partitionID                   = "x-opt-partition-id"
	ioTHubDeviceConnectionID      = "iothub-connection-device-id"
	ioTHubAuthGenerationID        = "iothub-connection-auth-generation-id"
	ioTHubConnectionAuthMethod    = "iothub-connection-auth-method"
	ioTHubConnectionModuleID      = "iothub-connection-module-id"
	ioTHubEnqueuedTime            = "iothub-enqueuedtime"
)

type EventHub struct {
	// Configuration
	ConnectionString          string   `toml:"connection_string"`
	ConsumerGroup             string   `toml:"consumer_group"`
	StorageContainerName      string   `toml:"storage_container_name"`
	BlobStoreConnectionString string   `toml:"blob_store_connection_string"`
	PersistenceDir            string   `toml:"persistence_dir"`
	MessageCount              int      `toml:"message_count"`
	TimeoutMessageReceiveSec  int      `toml:"timeout_message_receive_sec"`
	MaxUndeliveredMessages    int      `toml:"max_undelivered_messages"`
	UserAgent                 string   `toml:"user_agent"`
	PrefetchCount             int32    `toml:"prefetch_count"`
	PartitionIDs              []string `toml:"partition_ids"`
	Latest                    bool     `toml:"latest"`
	EnqueuedTimeAsTS          bool     `toml:"enqueued_time_as_ts"`
	IotHubEnqueuedTimeAsTS    bool     `toml:"iot_hub_enqueued_time_as_ts"`

	// Metadata
	ApplicationPropertyFields     []string `toml:"application_property_fields"`
	ApplicationPropertyTags       []string `toml:"application_property_tags"`
	SequenceNumberField           string   `toml:"sequence_number_field"`
	EnqueuedTimeField             string   `toml:"enqueued_time_field"`
	OffsetField                   string   `toml:"offset_field"`
	PartitionIDTag                string   `toml:"partition_id_tag"`
	PartitionKeyTag               string   `toml:"partition_key_tag"`
	IoTHubDeviceConnectionIDTag   string   `toml:"iot_hub_device_connection_id_tag"`
	IoTHubAuthGenerationIDTag     string   `toml:"iot_hub_auth_generation_id_tag"`
	IoTHubConnectionAuthMethodTag string   `toml:"iot_hub_connection_auth_method_tag"`
	IoTHubConnectionModuleIDTag   string   `toml:"iot_hub_connection_module_id_tag"`
	IoTHubEnqueuedTimeField       string   `toml:"iot_hub_enqueued_time_field"`

	Log telegraf.Logger `toml:"-"`

	// Azure
	consumerClient  *azeventhubs.ConsumerClient
	checkpointStore azeventhubs.CheckpointStore

	cancel context.CancelFunc
	wg     sync.WaitGroup

	parser telegraf.Parser
	in     chan []telegraf.Metric
}

type (
	empty     struct{}
	semaphore chan empty
)

func (*EventHub) SampleConfig() string {
	return sampleConfig
}

func (e *EventHub) Init() (err error) {
	if e.MaxUndeliveredMessages == 0 {
		e.MaxUndeliveredMessages = defaultMaxUndeliveredMessages
	}

	if e.MessageCount == 0 {
		e.Log.Debug("message count can not be 0, using default - 100")
		e.MessageCount = 100
	}

	if e.TimeoutMessageReceiveSec == 0 {
		e.Log.Debug("timeout message receive sec can not be 0, using default - 60")
		e.TimeoutMessageReceiveSec = 60
	}

	// Validate that only one persistence method is used
	if e.BlobStoreConnectionString != "" && e.PersistenceDir != "" {
		return fmt.Errorf("blob_store_connection_string and persistence_dir are mutually exclusive, please choose one")
	}

	if e.PersistenceDir != "" {
		e.Log.Debugf("Enable eventhub persistance with local dir: %s", e.PersistenceDir)

		checkpointStore, err := NewFileStore(e.PersistenceDir)
		if err != nil {
			return fmt.Errorf("error creating file store for checkpointing: %w", err)
		}

		e.checkpointStore = checkpointStore
	}

	// Run eventhub consumer with persistence
	if e.BlobStoreConnectionString != "" {
		if e.StorageContainerName == "" {
			return fmt.Errorf("storage_container_name must be set when using blob_store_connection_string")
		}

		e.Log.Debugf("Enable eventhub persistance with storage container: %s", e.StorageContainerName)

		blobClient, err := azblob.NewClientFromConnectionString(e.BlobStoreConnectionString, nil)
		if err != nil {
			return fmt.Errorf("error creating blob storage client: %w", err)
		}

		// Create checkpoint
		azBlobContainerClient := blobClient.ServiceClient().NewContainerClient(e.StorageContainerName)
		checkpointStore, err := checkpoints.NewBlobStore(azBlobContainerClient, nil)
		if err != nil {
			return fmt.Errorf("error creating blob store for checkpointing: %w", err)
		}

		e.checkpointStore = checkpointStore
	}

	if e.ConsumerGroup == "" {
		e.ConsumerGroup = eventhub.DefaultConsumerGroup
	}

	namespace, eventhubName, connectionString := fetchEnvironmentVariables()

	options := &azeventhubs.ConsumerClientOptions{}

	options.ApplicationID = internal.ProductToken()
	if e.UserAgent != "" {
		options.ApplicationID = e.UserAgent
	}

	// Create event hub connection
	if e.ConnectionString != "" || connectionString != "" {
		if e.ConnectionString == "" {
			e.ConnectionString = connectionString
		}

		e.consumerClient, err = azeventhubs.NewConsumerClientFromConnectionString(e.ConnectionString, "", e.ConsumerGroup, options)
	} else {
		defaultAzureCred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return fmt.Errorf("error creating Azure token: %w", err)
		}

		if namespace == "" {
			return fmt.Errorf("environment variable EVENTHUB_NAMESPACE is not set")
		}

		if eventhubName == "" {
			return fmt.Errorf("environment variable EVENTHUB_NAME is not set")
		}

		e.consumerClient, err = azeventhubs.NewConsumerClient(namespace, eventhubName, e.ConsumerGroup, defaultAzureCred, options)
	}

	return err
}

func (e *EventHub) Start(acc telegraf.Accumulator) error {
	e.in = make(chan []telegraf.Metric)

	var ctx context.Context
	ctx, e.cancel = context.WithCancel(context.Background())

	// Handle delivering messages
	e.wg.Go(func() {
		e.startTracking(ctx, acc)
	})

	processorOptions, partitionOptions := e.configureReceivers()

	// When persistance is enabled we have to create a processor that also brings in loadbalancing by default
	if e.checkpointStore != nil {
		processor, err := azeventhubs.NewProcessor(e.consumerClient, e.checkpointStore, processorOptions)
		if err != nil {
			return fmt.Errorf("error creating event hub processor: %w", err)
		}

		go e.dispatchProcessorClients(ctx, processor)

		if err := processor.Run(ctx); err != nil {
			return fmt.Errorf("error running event hub processor: %w", err)
		}
	} else {
		// Use single instance fetching without any persistance
		partitions := e.PartitionIDs
		if len(e.PartitionIDs) == 0 {
			runtimeinfo, err := e.consumerClient.GetEventHubProperties(ctx, nil)
			if err != nil {
				return err
			}

			partitions = runtimeinfo.PartitionIDs
		}

		partitionClients, err := e.dispatchPartitionClients(partitions, partitionOptions)
		if err != nil {
			return err
		}

		for partitionID, partitionClient := range partitionClients {
			go func() {
				if err := e.handlePartitionClient(ctx, partitionID, partitionClient); err != nil {
					e.Log.Errorf("Error handling partition client for partition %s: %v", &partitionID, partitionClient, err)
				}
			}()
		}
	}

	e.wg.Wait()

	return nil
}

func (e *EventHub) SetParser(parser telegraf.Parser) {
	e.parser = parser
}

func (*EventHub) Gather(telegraf.Accumulator) error {
	return nil
}

func (e *EventHub) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)

	e.consumerClient.Close(ctx)
	cancel()

	e.cancel()
	// Wait until all messages are processed
	e.wg.Wait()
}

func (e *EventHub) configureReceivers() (*azeventhubs.ProcessorOptions, *azeventhubs.PartitionClientOptions) {
	processorOptions := &azeventhubs.ProcessorOptions{}
	partitionOptions := &azeventhubs.PartitionClientOptions{}

	if e.PrefetchCount != 0 {
		processorOptions.Prefetch = e.PrefetchCount
		partitionOptions.Prefetch = e.PrefetchCount
	}

	if e.Latest {
		partitionOptions.StartPosition = azeventhubs.StartPosition{
			Latest: &e.Latest,
		}
	}

	return processorOptions, partitionOptions
}

func (e *EventHub) dispatchProcessorClients(ctx context.Context, processor *azeventhubs.Processor) {
	for {
		processorClient := processor.NextPartitionClient(ctx)
		if processorClient == nil {
			e.Log.Info("No more partition clients to process, exiting dispatcher")
			return
		}

		go func() {
			if err := e.handleProcessorClient(ctx, processorClient); err != nil {
				e.Log.Errorf("Error handling processor client for partition %s: %v", processorClient.PartitionID(), err)
			}
		}()
	}
}

func (e *EventHub) dispatchPartitionClients(partitions []string, partitionOptions *azeventhubs.PartitionClientOptions) (map[string]*azeventhubs.PartitionClient, error) {
	partitionClients := make(map[string]*azeventhubs.PartitionClient)
	for _, partitionID := range partitions {
		partitionClient, err := e.consumerClient.NewPartitionClient(partitionID, partitionOptions)
		if err != nil {
			return nil, fmt.Errorf("creating partition client for partition %q: %w", partitionID, err)
		}

		partitionClients[partitionID] = partitionClient
	}

	return partitionClients, nil
}

func (e *EventHub) handlePartitionClient(ctx context.Context, partitionID string, pc *azeventhubs.PartitionClient) error {
	defer func() {
		defer pc.Close(ctx)
		e.Log.Infof("Shutting down partition related resources for partition %s", partitionID)
	}()

	// Initialize partition related resources
	e.Log.Infof("Initializing partition related resources for partition %s", partitionID)
	// Receive messages
	e.Log.Infof("Starting to receive messages for partition %s", partitionID)

	for {
		// Wait until we received the configured number of events or running into the configured timout, otherwise returns whatever we collected during that time.
		receiveCtx, cancelReceive := context.WithTimeout(context.Background(), time.Duration(e.TimeoutMessageReceiveSec)*time.Second)
		events, err := pc.ReceiveEvents(receiveCtx, e.MessageCount, nil)
		cancelReceive()

		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			var eventHubError *azeventhubs.Error

			if errors.As(err, &eventHubError) && eventHubError.Code == azeventhubs.ErrorCodeOwnershipLost {
				return nil
			}

			return err
		}

		if len(events) == 0 {
			continue
		}

		e.Log.Debugf("messages received from partion %s - messages %d", partitionID, len(events))

		for _, event := range events {
			e.onMessage(ctx, event)
		}
	}
}

func (e *EventHub) handleProcessorClient(ctx context.Context, pc *azeventhubs.ProcessorPartitionClient) error {
	// Close client at the end of the run
	defer func() {
		defer pc.Close(ctx)
		e.Log.Info("Shutting down partition related resources for partition %s", pc.PartitionID())
	}()

	// Initialize partition related resources
	e.Log.Infof("Initializing partition related resources for partition %s", pc.PartitionID())

	// Receive messages
	e.Log.Infof("Starting to receive messages for partition %s", pc.PartitionID())
	for {
		// Wait until we received the configured number of events or running into the configured timout, otherwise returns whatever we collected during that time.
		receiveCtx, cancelReceive := context.WithTimeout(context.Background(), time.Duration(e.TimeoutMessageReceiveSec)*time.Second)
		events, err := pc.ReceiveEvents(receiveCtx, e.MessageCount, nil)
		cancelReceive()

		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			var eventHubError *azeventhubs.Error

			if errors.As(err, &eventHubError) && eventHubError.Code == azeventhubs.ErrorCodeOwnershipLost {
				return nil
			}

			return err
		}

		if len(events) == 0 {
			continue
		}

		e.Log.Debugf("messages received from partion %s - messages %d", pc.PartitionID(), len(events))

		for _, event := range events {
			e.onMessage(ctx, event)
		}

		// Updates the checkpoint with the latest event received. If processing needs to restart
		// it will restart from this point, automatically.
		checkpointContext, cancelContext := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelContext()

		if err := pc.UpdateCheckpoint(checkpointContext, events[len(events)-1], nil); err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				e.Log.Warnf("checkpoint could not be updated: %v", err)
				return nil
			}
			return err
		}
	}
}

// OnMessage handles an Event.  When this function returns without error the
// Event is immediately accepted and the offset is updated.  If an error is
// returned the Event is marked for redelivery.
func (e *EventHub) onMessage(ctx context.Context, event *azeventhubs.ReceivedEventData) error {
	metrics, err := e.createMetrics(event)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case e.in <- metrics:
		return nil
	}
}

// OnDelivery returns true if a new slot has opened up in the TrackingAccumulator.
func (e *EventHub) onDelivery(
	acc telegraf.TrackingAccumulator,
	groups map[telegraf.TrackingID][]telegraf.Metric,
	track telegraf.DeliveryInfo,
) bool {
	if track.Delivered() {
		delete(groups, track.ID())
		return true
	}

	// The metric was already accepted when onMessage completed, so we can't
	// fallback on redelivery from Event Hub.  Add a new copy of the metric for
	// reprocessing.
	metrics, ok := groups[track.ID()]
	delete(groups, track.ID())
	if !ok {
		// The metrics should always be found, this message indicates a programming error.
		e.Log.Errorf("Could not find delivery: %d", track.ID())
		return true
	}

	backup := deepCopyMetrics(metrics)
	id := acc.AddTrackingMetricGroup(metrics)
	groups[id] = backup
	return false
}

func (e *EventHub) startTracking(ctx context.Context, ac telegraf.Accumulator) {
	acc := ac.WithTracking(e.MaxUndeliveredMessages)
	sem := make(semaphore, e.MaxUndeliveredMessages)
	groups := make(map[telegraf.TrackingID][]telegraf.Metric, e.MaxUndeliveredMessages)

	for {
		select {
		case <-ctx.Done():
			return
		case track := <-acc.Delivered():
			if e.onDelivery(acc, groups, track) {
				<-sem
			}
		case sem <- empty{}:
			select {
			case <-ctx.Done():
				return
			case track := <-acc.Delivered():
				if e.onDelivery(acc, groups, track) {
					<-sem
					<-sem
				}
			case metrics := <-e.in:
				backup := deepCopyMetrics(metrics)
				id := acc.AddTrackingMetricGroup(metrics)
				groups[id] = backup
			}
		}
	}
}

// CreateMetrics returns the Metrics from the Event.
func (e *EventHub) createMetrics(event *azeventhubs.ReceivedEventData) ([]telegraf.Metric, error) {
	metrics, err := e.parser.Parse(event.EventData.Body)
	if err != nil {
		return nil, err
	}

	if len(metrics) == 0 {
		once.Do(func() {
			e.Log.Debug(internal.NoMetricsCreatedMsg)
		})
	}

	for i := range metrics {
		for _, field := range e.ApplicationPropertyFields {
			if val, ok := event.Properties[field]; ok {
				metrics[i].AddField(field, val)
			}
		}

		for _, tag := range e.ApplicationPropertyTags {
			if val, ok := event.Properties[tag]; ok {
				metrics[i].AddTag(tag, fmt.Sprintf("%v", val))
			}
		}

		if e.SequenceNumberField != "" && event.SequenceNumber != 0 {
			metrics[i].AddField(e.SequenceNumberField, event.SequenceNumber)
		}

		if event.EnqueuedTime != nil {
			if e.EnqueuedTimeAsTS {
				metrics[i].SetTime(*event.EnqueuedTime)
			} else if e.EnqueuedTimeField != "" {
				metrics[i].AddField(e.EnqueuedTimeField, (*event.EnqueuedTime).UnixNano()/int64(time.Millisecond))
			}
		}

		if e.OffsetField != "" && event.Offset != "" {
			metrics[i].AddField(e.OffsetField, event.Offset)
		}

		if value, ok := event.SystemProperties[partitionID]; ok && e.PartitionIDTag != "" {
			metrics[i].AddTag(e.PartitionIDTag, strconv.Itoa(int(value.(int32))))
		}

		if e.PartitionKeyTag != "" && event.PartitionKey != nil {
			metrics[i].AddTag(e.PartitionKeyTag, *event.PartitionKey)
		}

		if value, ok := event.SystemProperties[ioTHubDeviceConnectionID]; ok && e.IoTHubDeviceConnectionIDTag != "" {
			metrics[i].AddTag(e.IoTHubDeviceConnectionIDTag, value.(string))
		}

		if value, ok := event.SystemProperties[ioTHubAuthGenerationID]; ok && e.IoTHubAuthGenerationIDTag != "" {
			metrics[i].AddTag(e.IoTHubAuthGenerationIDTag, value.(string))
		}

		if value, ok := event.SystemProperties[ioTHubConnectionAuthMethod]; ok && e.IoTHubConnectionAuthMethodTag != "" {
			metrics[i].AddTag(e.IoTHubConnectionAuthMethodTag, value.(string))
		}

		if value, ok := event.SystemProperties[ioTHubConnectionModuleID]; ok && e.IoTHubConnectionModuleIDTag != "" {
			metrics[i].AddTag(e.IoTHubConnectionModuleIDTag, value.(string))
		}
		if value, ok := event.SystemProperties[ioTHubEnqueuedTime]; ok && value != nil {
			if e.IotHubEnqueuedTimeAsTS {
				metrics[i].SetTime(value.(time.Time))
			} else if e.IoTHubEnqueuedTimeField != "" {
				metrics[i].AddField(e.IoTHubEnqueuedTimeField, (value.(time.Time)).UnixNano()/int64(time.Millisecond))
			}
		}
	}

	return metrics, nil
}

func init() {
	inputs.Add("eventhub_consumer", func() telegraf.Input {
		return &EventHub{}
	})
}

func deepCopyMetrics(in []telegraf.Metric) []telegraf.Metric {
	metrics := make([]telegraf.Metric, 0, len(in))
	for _, m := range in {
		metrics = append(metrics, m.Copy())
	}
	return metrics
}

func fetchEnvironmentVariables() (string, string, string) {
	namespace := os.Getenv("EVENTHUB_NAMESPACE")
	name := os.Getenv("EVENTHUB_NAME")
	connectionString := os.Getenv("EVENTHUB_CONNECTION_STRING")

	return namespace, name, connectionString
}

// Custom FileStore implementation for local file-based checkpointing
type FileStore struct {
	// Add necessary fields here
	persistenceDir string
}

func NewFileStore(persistenceDir string) (*FileStore, error) {
	if _, err := os.Stat(persistenceDir); os.IsNotExist(err) {
		ownershipDir := filepath.Join(persistenceDir, "ownerships")
		if err := os.MkdirAll(ownershipDir, 0750); err != nil {
			return nil, fmt.Errorf("error creating persistence/ownerships directory: %w", err)
		}

		checkpointsDir := filepath.Join(persistenceDir, "checkpoints")
		if err := os.MkdirAll(checkpointsDir, 0750); err != nil {
			return nil, fmt.Errorf("error creating persistence/checkpoints directory: %w", err)
		}
	}

	return &FileStore{
		persistenceDir: persistenceDir,
	}, nil
}

// Implement necessary methods for FileStore to satisfy the checkpoints.Store interface
func (fs *FileStore) ClaimOwnership(ctx context.Context, partitionOwnership []azeventhubs.Ownership, options *azeventhubs.ClaimOwnershipOptions) ([]azeventhubs.Ownership, error) {
	var ownerships []azeventhubs.Ownership

	//loop over all checkpoint files in the persistence dir and create checkpoints.
	pRootDir := filepath.Join(fs.persistenceDir, "ownerships")

	for _, po := range partitionOwnership {
		data, err := json.Marshal(po)
		if err != nil {
			return nil, fmt.Errorf("error marshaling ownership: %w", err)
		}

		filePath := fmt.Sprintf("%s/ownership_%s_%s_%s_%s.json", pRootDir, po.FullyQualifiedNamespace, po.EventHubName, po.ConsumerGroup, po.PartitionID)
		if err := os.WriteFile(filePath, data, 0750); err != nil {
			return nil, fmt.Errorf("error writing ownership file: %w", err)
		}

		//Just add the unmodified ownership because it's a local store
		ownerships = append(ownerships, po)
	}

	return ownerships, nil
}

func (fs *FileStore) ListCheckpoints(ctx context.Context, fullyQualifiedNamespace string, eventHubName string, consumerGroup string, options *azeventhubs.ListCheckpointsOptions) ([]azeventhubs.Checkpoint, error) {
	var checkpoints []azeventhubs.Checkpoint

	//loop over all checkpoint files in the persistence dir and create checkpoints.
	pRootDir := os.DirFS(filepath.Join(fs.persistenceDir, "checkpoints"))

	// get all json files where the checkpoints are stored
	checkpointFiles, err := ioFs.Glob(pRootDir, "*.json")
	if err != nil {
		return nil, fmt.Errorf("error listing checkpoint files: %w", err)
	}

	for _, cpFile := range checkpointFiles {
		data, err := os.ReadFile(filepath.Join(fs.persistenceDir, "checkpoints", cpFile))
		if err != nil {
			return nil, fmt.Errorf("error reading checkpoint file %s: %w", cpFile, err)
		}

		loadedCheckpoint := azeventhubs.Checkpoint{}
		json.Unmarshal(data, &loadedCheckpoint)
		checkpoints = append(checkpoints, loadedCheckpoint)
	}

	return checkpoints, nil
}

func (fs *FileStore) ListOwnership(ctx context.Context, fullyQualifiedNamespace string, eventHubName string, consumerGroup string, options *azeventhubs.ListOwnershipOptions) ([]azeventhubs.Ownership, error) {
	var ownerships []azeventhubs.Ownership

	ownershipsDir := filepath.Join(fs.persistenceDir, "ownerships")
	ownershipsDirFs := os.DirFS(ownershipsDir)

	filePattern := fmt.Sprintf("ownership_%s_%s_%s_*.json", fullyQualifiedNamespace, eventHubName, consumerGroup)

	ownershipFiles, err := ioFs.Glob(ownershipsDirFs, filePattern)
	if err != nil {
		return nil, fmt.Errorf("error listing ownership files: %w", err)
	}

	//loop over all ownership files in the persistence dir
	for _, osFile := range ownershipFiles {
		data, err := os.ReadFile(filepath.Join(ownershipsDir, osFile))
		if err != nil {
			return nil, fmt.Errorf("error reading ownership file %s: %w", osFile, err)
		}

		loadedOwnership := azeventhubs.Ownership{}
		json.Unmarshal(data, &loadedOwnership)
		ownerships = append(ownerships, loadedOwnership)
	}

	// Implement file-based ownership listing logic here
	return ownerships, nil
}

func (fs *FileStore) SetCheckpoint(ctx context.Context, checkpoint azeventhubs.Checkpoint, options *azeventhubs.SetCheckpointOptions) error {
	//Persist checkpoint struckt as a json file to the persistence dir.
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("error marshaling checkpoint: %w", err)
	}

	filePath := fmt.Sprintf("%s/checkpoint_%s_%s_%s.json", filepath.Join(fs.persistenceDir, "checkpoints"), checkpoint.FullyQualifiedNamespace, checkpoint.EventHubName, checkpoint.PartitionID)
	if err := os.WriteFile(filePath, data, 0750); err != nil {
		return fmt.Errorf("error writing checkpoint file: %w", err)
	}

	return nil
}
