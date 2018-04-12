package org.krangl.performance.playground;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.openjdk.jmh.annotations.*;

import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * http://codingjunkie.net/micro-benchmarking-with-caliper/
 * <p>
 * Cool example https://github.com/eugenp/tutorials/blob/master/jmh/src/main/java/com/baeldung/BenchMark.java
 *
 * @author Holger Brandl
 */

@Warmup(iterations = 5)
public class ExampleBenchmark {

    @Benchmark
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1, warmups = 1)
    @Measurement(iterations = 5)
//    @BenchmarkMode(Mode.Throughput)
//    @BenchmarkMode(Mode.AverageTime)
    @Warmup(iterations = 5)
    public void benchMurmur3_128(ExecutionPlan plan) {

        for (int i = plan.iterations; i > 0; i--) {
            plan.murmur3.putString(plan.password, Charset.defaultCharset());
        }

        plan.murmur3.hash();
    }


    //    @Benchmark
    @Fork(value = 1, warmups = 1)
    @Measurement(iterations = 5)
    @BenchmarkMode(Mode.SampleTime)
    public void init() throws InterruptedException {
        // Do nothing
        Thread.sleep((long) (new Random().nextFloat() * 1000));
    }


    //    @Benchmark
    @Fork(value = 1, warmups = 1)
    @Measurement(iterations = 5)
    @BenchmarkMode(Mode.AverageTime)
    public void init2() throws InterruptedException {
        // Do nothing
        Thread.sleep((long) (new Random().nextFloat() * 1000));
    }


    @State(Scope.Benchmark)
    public static class ExecutionPlan {

        public Hasher murmur3;

//        @Param({"foo", "bar"})
//        String something;
        public String password = "4v3rys3kur3p455w0rd";
        //        @Param({ "100", "200", "300", "500", "1000" })
        @Param({"1", "2"})
        int iterations;


        @Setup(Level.Invocation)
        public void setUp() {
            murmur3 = Hashing.murmur3_128().newHasher();
        }
    }


//    public static class BenchmarkRunner {
//
//        // does not work without jar deployment, see http://openjdk.java.net/projects/code-tools/jmh/
//        public static void main(String[] args) throws Exception {
//            org.openjdk.jmh.Main.main(args);
//        }
//
//    }
}
