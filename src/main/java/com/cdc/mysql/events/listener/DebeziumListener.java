package com.cdc.mysql.events.listener;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.stereotype.Component;

import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DebeziumListener {

    private final Executor executor = Executors.newSingleThreadExecutor();    
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;

    public DebeziumListener(Configuration userConnector) {
        this.debeziumEngine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
            .using(userConnector.asProperties())
            .notifying(this::handleChangeEvent)
            .build();
    }

    private void handleChangeEvent(RecordChangeEvent<SourceRecord> sourceRecordRecordChangeEvent) {
    	var sourceRecord = sourceRecordRecordChangeEvent.record();
		Struct sourceRecordChangeValue = (Struct) sourceRecord.value();
		Map<String, Object> payload = sourceRecordChangeValue.schema().fields().stream().map(Field::name)
				.filter(fieldName -> sourceRecordChangeValue.get(fieldName) != null)
				.map(fieldName -> Pair.of(fieldName, sourceRecordChangeValue.get(fieldName)))
				.collect(Collectors.toMap(Pair::getKey, Pair::getValue));
		String op = (String) payload.get("__op");
		String tableName = (String) payload.get("__table");
		payload.remove("__op");
		payload.remove("__table");
		System.out.println("payload:: " + payload);
		System.out.println(op+"::"+tableName);
    }

    @PostConstruct
    private void start() {
        this.executor.execute(debeziumEngine);
    }

    @PreDestroy
    private void stop() throws IOException {
        if (Objects.nonNull(this.debeziumEngine)) {
            this.debeziumEngine.close();
        }
    }

}