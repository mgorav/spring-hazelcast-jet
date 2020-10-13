package com.gonnect.hazelcast.jet.peipeline;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class HazelcastJetPipelineApplication {
    @Autowired
    JetInstance instance;

    public static void main(String[] args) {
        SpringApplication.run(HazelcastJetPipelineApplication.class, args);
    }

    @RequestMapping("/submitJob")
    public void submitJob() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items("Apple", "Mangoes"))
                .writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig()
                .addClass(HazelcastJetPipelineApplication.class);
        instance.newJob(pipeline, jobConfig).join();
    }
}
