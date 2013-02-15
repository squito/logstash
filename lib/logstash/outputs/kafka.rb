require "logstash/outputs/base"
require "logstash/namespace"
require "kafka"

# Write events through Kafka.
#
class LogStash::Outputs::Kafka < LogStash::Outputs::Base

  config_name "kafka"
  plugin_status "beta"

  # The address to connect to.
  config :host, :validate => :string, :required => true

  # The port to connect to.
  config :port, :validate => :number, :required => true

  # The Kafka Topic.
  config :topic, :validate => :string, :default => "logstash"

  # The format to use when writing events to the file. This value
  # supports any string and can include %{name} and other dynamic
  # strings.
  #
  # If this setting is omitted, the full json representation of the
  # event will be written as a single line.
  config :message_format, :validate => :string

  public
  def register
    @producer = Kafka::Producer.new(:topic => @topic, :host => @host, :port => @port)
  end

  public
  def receive(event)
    @producer.send([Kafka::Message.new(event.to_hash.to_json)])
  end # def receive
end # class LogStash::Outputs::Kafka
