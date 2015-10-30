# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "logstash/util/fieldreference"


# Search elasticsearch for a previous log event and copy some fields from it
# into the current event.  Below is a complete example of how this filter might
# be used.  Whenever logstash receives an "end" event, it uses this elasticsearch
# filter to find the matching "start" event based on some operation identifier.
# Then it copies the @timestamp field from the "start" event into a new field on
# the "end" event.  Finally, using a combination of the "date" filter and the
# "ruby" filter, we calculate the time duration in hours between the two events.
#
#       if [type] == "end" {
#          elasticsearch {
#             hosts => ["es-server"]
#             query => "type:start AND operation:%{[opid]}"
#             fields => ["@timestamp", "started"]
#          }
#
#          date {
#             match => ["[started]", "ISO8601"]
#             target => "[started]"
#          }
#
#          ruby {
#             code => "event['duration_hrs'] = (event['@timestamp'] - event['started']) / 3600 rescue nil"
#          }
#       }
#
class LogStash::Filters::ElasticsearchRespTime < LogStash::Filters::Base
  config_name "elasticsearch_resp_time"
  milestone 1

  # List of elasticsearch hosts to use for querying.
  config :hosts, :validate => :array

  # Elasticsearch query string
  config :query, :validate => :string

  # Comma-delimited list of <field>:<direction> pairs that define the sort order
  config :sort, :validate => :string, :default => "@timestamp:desc"

  # Hash of fields to copy from old event (found via elasticsearch) into new event
  # config :fields, :validate => :hash, :default => {}

  # [EBO-20150205] Add _source filtering field
  config :source_filter, :validate => :string

  # [EBO-20150205] Add percentage limit
  config :percentage_limit, :validate => :number, :default => 5
  
  # [EBO-20150205] Add best_response_time_field_name
  config :best_response_time_field_name, :validate => :string, :default => "best_response_time"
  
  # [EBO-20150205] Add worst_response_time_field_name
  config :worst_response_time_field_name, :validate => :string, :default => "worst_response_time"

  
  public
  def register
    require "elasticsearch"

    @logger.info("New ElasticSearchRespTime filter", :hosts => @hosts)
    @client = Elasticsearch::Client.new hosts: @hosts
  end # def register

  public
  def filter(event)
    return unless filter?(event)

    begin
      query_str = event.sprintf(@query)
      source_filter = @source_filter != "" ? event.sprintf(@source_filter) : true
      
      results = @client.search q: query_str, sort: @sort, _source: source_filter, size: 10000

      if (results['hits']['total'] == 0) 
         event[@best_response_time_field_name] = 0.0
         event[@worst_response_time_field_name] = 0.0
      elsif (results['hits']['total'] == 1)
         event[@best_response_time_field_name] = results['hits']['hits'][0]['_source']['latency']['response_transmitted']
         event[@worst_response_time_field_name] = results['hits']['hits'][0]['_source']['latency']['response_transmitted']
      else 
         limit_record =  (((results['hits']['total'].to_f * @percentage_limit.to_f) - 0.1) / 100.0).floor
         worst_response_time = 0
         for i in 0..limit_record
            worst_response_time += results['hits']['hits'][i]['_source']['latency']['response_transmitted']
         end
         worst_response_time = worst_response_time.to_f / (limit_record + 1).to_f
         event[@worst_response_time_field_name] = worst_response_time

         best_response_time = 0
         for i in limit_record+1..results['hits']['total']-1
            best_response_time += results['hits']['hits'][i]['_source']['latency']['response_transmitted']
         end
         best_response_time = best_response_time.to_f / (results['hits']['total']-limit_record).to_f
         event[@best_response_time_field_name] = best_response_time
          
      end

      filter_matched(event)
    rescue => e
      @logger.warn("Failed to query elasticsearch for previous event",
                   :query => query_str, :error => e)
    end
  end # def filter
end # class LogStash::Filters::ElasticsearchRespTime