require "logstash/inputs/base"
require "logstash/namespace"
require 'kafka'
  
# Read events through Kafka.
#
class LogStash::Inputs::Kafka < LogStash::Inputs::Base
  class Interrupted < StandardError; end
  config_name "kafka"
  plugin_status 0

  # The address to connect to.
  config :host, :validate => :string, :default => "0.0.0.0"
  
  # The port to connect to.
  config :port, :validate => :number, :required => true

  config :topic, :validate => :string, :default => "logstash"

  public
  def register
    @consumer = Kafka::Consumer.new(:topic => @topic, :host => @host, :port => @port)
  end # def register

  public
  def run(output_queue)
    @consumer.loop do |messages|
      messages.each do |msg|
        queue_event(msg.payload, output_queue) 
      end
    end
    finished
  end # def run

  private
  def queue_event(msg, output_queue)
    begin
      @codec.decode(msg) do |event|
        decorate(event)
        output_queue << event
      end
    rescue => e # parse or event creation error
      @logger.error("Failed to create event", :message => msg, :exception => e,
                    :backtrace => e.backtrace);
    end
  end

end # class LogStash::Inputs::Kafka
