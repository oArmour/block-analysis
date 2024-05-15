package com.oarmour.cep;

import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CEPRawCase {

    @Test
    public void testBase1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism to 1
        env.setParallelism(1);

        // Create input sequence
        DataStream<Event> input = env.fromElements(
                new Event(1, "a"),
                new Event(2, "c"),
                new Event(1, "b1"),
                new Event(3, "b2"),
                new Event(4, "d"),
                new Event(4, "b3")
        );

        // ------ Create pattern "a b" ------
        Pattern<Event, ?> start = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value){
                return value.getName().startsWith("a");
            }
        });

        // Strict contiguity
        Pattern<Event, ?> strict = start.next("next").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value){
                return value.getName().startsWith("b");
            }
        });

        // Relaxed contiguity
        Pattern<Event, ?> relaxed = start.followedBy("next").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value){
                return value.getName().startsWith("b");
            }
        });

        // Non - Deterministic Relaxed contiguity
        Pattern<Event, ?> nonDRelaxed = start.followedByAny("next").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value){
                return value.getName().startsWith("b");
            }
        });

        // Choose contiguity condition
        Pattern<Event, ?> pattern = strict;

        PatternStream<Event> patternStream = org.apache.flink.cep.CEP.pattern(input, pattern);

        // Create result with matches
        DataStream<String> result = patternStream.select((Map<String, List<Event>> p) -> {
            String strResult = "";
            // Check if sth equals null so that the optional() quantifier can be used
            if (p.get("start") != null){
                for (int i = 0; i < p.get("start").size(); i++){ // for looping patterns
                    strResult += p.get("start").get(i).getName() + " ";
                }
            }
            if (p.get("next") != null){
                for (int i = 0; i < p.get("next").size(); i++){
                    strResult += p.get("next").get(i).getName() + " ";
                }
            }
            return strResult;
        });

        // Print matches
        result.print();

        env.execute("Flink CEP Contiguity Conditions, Simple Pattern Example");
    }
}

class Event{

    private String name;
    private int id;

    public Event(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return ("Event(" + id + ", " + name + ")");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Event) {
            Event other = (Event) obj;

            return name.equals(other.name) && id == other.id;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id);
    }
}
