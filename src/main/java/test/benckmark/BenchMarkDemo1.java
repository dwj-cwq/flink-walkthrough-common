package test.benckmark;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * @author zhang lianhui
 * @date 2020/11/12 11:06 上午
 */
public class BenchMarkDemo1 {

	@Fork(value = 1, warmups = 1)
	@Benchmark
	@BenchmarkMode(Mode.Throughput)
	public void benchMurmur3_128(ExecutionPlan plan) {

		for (int i = plan.iterations; i > 0; i--) {
			plan.murmur3.putString(plan.password, Charset.defaultCharset());
		}

		plan.murmur3.hash();
	}


	@State(Scope.Benchmark)
	public static class ExecutionPlan {

		@Param({"100", "200", "300", "500", "1000"})
		public int iterations;

		public Hasher murmur3;

		public String password = "4v3rys3kur3p455w0rd";

		@Setup(Level.Invocation)
		public void setUp() {
			murmur3 = Hashing.murmur3_128().newHasher();
		}
	}

	public static void main(String[] args) throws IOException, RunnerException {
		org.openjdk.jmh.Main.main(args);
	}
}
