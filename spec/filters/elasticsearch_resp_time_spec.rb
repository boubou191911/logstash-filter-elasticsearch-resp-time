require 'spec_helper'
require "logstash/filters/elasticsearch_resp_time"

describe LogStash::Filters::ElasticsearchRespTime do
  context "registration" do

    let(:plugin) { LogStash::Plugin.lookup("filter", "elasticsearch_resp_time").new({}) }

    it "should not raise an exception" do
      expect {plugin.register}.to_not raise_error
    end
  end
end
