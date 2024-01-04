package org.acme.kafka.streams.aggregator;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Properties;
import org.json.JSONObject;

@ApplicationScoped
public class TopologyProducer {
    @Inject
    Logger log;

    static final String STATUS_STORE = "StatusStore";

    private static final String STATUS_TOPIC = "um-ibdi-comm-status";
    private static final String SPC_TOPIC = "um-ibdi-comm-spc-result";
    private static final String NCN_TOPIC = "um-ibdi-comm-ncn-result";
    private static final String CPV_TOPIC = "um-ibdi-comm-cpv-result";

    private static final String REPORT_TOPIC = "um-ibdi-comm-report";

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> statusStream = builder.stream(STATUS_TOPIC);
        KTable<String, String> statusTable = statusStream.groupByKey()
            .aggregate(
                () -> "" , 
                (aggKey, newValue, aggValue) -> newValue, 
                Materialized.as(STATUS_STORE));

        // ArrayList<String> topics = new ArrayList<String>();
        // topics.add("um-ibdi-comm-status");
        // topics.add("um-ibdi-comm-cpv");
        // topics.add("um-ibdi-comm-ncn");
        // topics.add("um-ibdi-comm-spc");
        // KStream<String, String> cogroupedStream = builder.stream(topics);
        KStream<String, String> spcStream = builder.stream(SPC_TOPIC);
        //spcStream.peek((key, value) -> log.info("spcStream record - key " + key + ", value " + value));
        KGroupedStream<String, String> spcGroupedStream = spcStream.groupByKey();
        
        KStream<String, String> ncnStream = builder.stream(NCN_TOPIC);
        KGroupedStream<String, String> ncnGroupedStream = ncnStream.groupByKey();

        KStream<String, String> cpvStream = builder.stream(CPV_TOPIC);
        KGroupedStream<String, String> cpvGroupedStream = cpvStream.groupByKey();

        CogroupedKStream<String, String> cogroupedStream = spcGroupedStream.cogroup(commAggregator)
            .cogroup(ncnGroupedStream, commAggregator)
            .cogroup(cpvGroupedStream, commAggregator);
        KTable<String, String> coTable = cogroupedStream
            .aggregate(() -> "")
            .join(statusTable, 
                new ValueJoiner<String, String, String>() {
                    @Override
                    public String apply(String aggValue, String statusValue) {
                        JSONObject aggJsonObj = new JSONObject(aggValue.toString());
                        JSONObject statusJsonObj = new JSONObject(statusValue.toString());
                        int checkCount = Integer.valueOf(aggJsonObj.getString("checkCount"));
                        int count = Integer.valueOf(statusJsonObj.getString("count"));
                        //System.out.println("Count check ===> " + checkCount + ", statusCount ===> " + count);
                        log.info("Count check ===> " + checkCount + ", statusCount ===> " + count);
                        
                        //if (aggJsonObj.getString("checkCount").equals(statusJsonObj.getString("count"))) {
                        if (checkCount == count) {
                            //System.out.println("aggJsonObj ===> " + aggJsonObj.toString());
                            log.info("aggJsonObj ===> " + aggJsonObj.toString());
                            aggJsonObj.put("mesResult", statusJsonObj.get("result"));
                            aggJsonObj.put("event", "completed");
                            
                            return aggJsonObj.toString();
                        }

                        return null;
                    }
                });

        coTable.toStream()
			//.peek((key, value) -> log.info("join record - key " + key + ", value " + value))
            .groupByKey().aggregate(
                () -> "", 
                (aggKey, newValue, aggValue) -> newValue)
            .toStream()
            .peek((key, value) -> log.info("Outgoing record - key " + key))
            .to(REPORT_TOPIC);

        return builder.build();
    }

    private Aggregator<String, String, String> commAggregator = new Aggregator<String, String, String>() { 
		@Override
		public String apply(String aggKey, String newValue, String aggValue) {
			JSONObject aggJsonObj = new JSONObject();
			int count = 1;
			if (!aggValue.equals("")) {
				aggJsonObj = new JSONObject(aggValue);
				count = aggJsonObj.getInt("checkCount") + 1;
			} else {
				aggJsonObj.put("batch_id", aggKey);
			};

			JSONObject newJsonValue = new JSONObject(newValue);
			
			aggJsonObj.put("batch_id", aggKey);
			switch (newJsonValue.get("source").toString()) {
				case "SPC":
					aggJsonObj.put("spcResult", newJsonValue.get("result"));
					break;
				case "NCN":
					aggJsonObj.put("ncnResult", newJsonValue.get("result"));
					break;
				case "CPV":
					aggJsonObj.put("cpvResult", newJsonValue.get("result"));
					break;
				default:
					break;
			}
			
			aggJsonObj.put("checkCount", String.valueOf(count));
            
			return aggJsonObj.toString();
		}
	};
}