package datapipe

// MQTTClient defines the interface for MQTT operations
type MQTTClient interface {
	Subscribe(topic string, qos byte) error
	Unsubscribe(topic string) error
}
