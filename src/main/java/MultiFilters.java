import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.Random;    
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;   
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
//import org.apache.log4j.Logger;
//import org.apache.logging.log4j.Logger;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Level;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.Host;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;

public class MultiFilters {
	//private static Logger log = LogManager.getLogger(MultiFilters.class);
	private AerospikeClient client;
	private Policy policy;
	private WritePolicy writePolicy;
	private Host hosts[];
	private int failCount = 1;
	private int nthreads = 50;
	private int threadCount = 50;
	private int target = 50;
	private int repeatCount = 1;
	private int partitionRange[] = null;
	private Map<String, Boolean> wbool = new HashMap<String, Boolean>();
	//private boolean withDetail = false;
	//private boolean withReassign = false;
	//private boolean withRestore = false;
	//private boolean withLoad = false;

	public MultiFilters(String seedHost, int port) throws AerospikeException{
		//this.policy = new Policy();
		this.writePolicy = new WritePolicy();
		this.hosts = new Host[] {new Host(seedHost, port)};
		//this.client = new AerospikeClient(seedHost, port);
	}

	public void setHosts() {
		this.hosts = new Host[] {
			new Host("node01", 3000),
			new Host("node02", 3000),
			new Host("node03", 3000),
			new Host("node04", 3000),
			new Host("node05", 3000),
			new Host("node06", 3000),
			new Host("node07", 3000),
			new Host("node08", 3000),
			new Host("node09", 3000),
			new Host("node10", 3000)
		};
	}

	public Host[] getHosts() {
		return this.hosts;
	}

	public void printHosts() {
		for (Host host: this.getHosts()) {
			System.out.println("  " + host);
		}
	}

	public static void main(String[] args) throws AerospikeException, ParseException{
		Options options = new Options();
		options.addOption("h", "host", true, "Server hostname (default: localhost)");
		options.addOption("p", "port", true, "Server port (default: 3000)");
		options.addOption("f", "fail", true, "Fail count (default: 1)");
		options.addOption("t", "thread", true, "Threads count (default: cpu_number)");
		options.addOption("g", "target", true, "Target throughput (default: 0)");
		options.addOption("P", "partitionRange", true, "partition range for scanning (default:0-4096)");
		options.addOption("r", "repeat", true, "Repeat count (default: 1)");
		options.addOption("d", "WithDetail", false, "With detail info or not (default: false)");
		options.addOption("l", "withLoad", false, "With registration data load or not (default: false)");
		options.addOption("a", "withReassign", false, "With reassign or not (default: false)");
		options.addOption("s", "withRestore", false, "With registration data restore or not (default: false)");
		options.addOption("u", "usage", false, "Print usage.");

		CommandLineParser parser = new PosixParser();
		CommandLine cl = parser.parse(options, args, false);

		if (args.length == 0 || cl.hasOption("u")) {
			logUsage(options);
			return;
		}

		int partitionRange[] = null;
		String partitionRangeString[] = cl.getOptionValue("P", "0-4096").split("-");
		partitionRange = new int[2];
		partitionRange[0] = Integer.parseInt(partitionRangeString[0].trim());
		partitionRange[1] = Integer.parseInt(partitionRangeString[1].trim());

		String host = cl.getOptionValue("h", "127.0.0.1");
		String portString = cl.getOptionValue("p", "3000");
		String failCountString = cl.getOptionValue("f", "1");
		String threadCountString = cl.getOptionValue("t", Integer.toString(Runtime.getRuntime().availableProcessors()));
		String targetString = cl.getOptionValue("g", "0");
		String repeatCountString = cl.getOptionValue("r", "1");
		int port = Integer.parseInt(portString);
		int failCount = Integer.parseInt(failCountString);
		int threadCount = Integer.parseInt(threadCountString);
		int target = Integer.parseInt(targetString);
		int repeatCount = Integer.parseInt(repeatCountString);
		boolean withHost =  cl.hasOption("h") ? true:false;
		boolean withDetail =  cl.hasOption("d") ? true:false;
		boolean withReassign =  cl.hasOption("a") ? true:false;
		boolean withLoad =  cl.hasOption("l") ? true:false;
		boolean withRestore =  cl.hasOption("s") ? true:false;

		if (withLoad) {
			withReassign = false;
			withRestore = false;
		}

		if (withRestore) {
			withLoad = false;
		}

		Map<String, Boolean> wbool = new HashMap<String, Boolean>();
		wbool.put("withDetail", withDetail);
		wbool.put("withLoad", withLoad);
		wbool.put("withReassign", withReassign);
		wbool.put("withRestore", withRestore);

		MultiFilters mf = new MultiFilters(host, port);
		mf.setArgs(failCount, threadCount, target, repeatCount, partitionRange, wbool);
		if (!withHost) {
			mf.setHosts();
		}

		System.out.println("Parameters:");
		//System.out.println("Host: " + mf.getHosts());
		//System.out.println("Port: " + port);
		System.out.println("Hosts:");
		mf.printHosts();
		System.out.println("Fail count: " + failCount);
		System.out.printf("Partition range: [%d, %d)\n", partitionRange[0], partitionRange[1]);
		System.out.println("Repeat count: " + repeatCount);
		System.out.println("Target TPS: " + target);
		System.out.println("With detail: " + withDetail);
		System.out.println("With reg load: " + withLoad);
		System.out.println("With reassignment: " + withReassign);
		System.out.println("With reg restore: " + withRestore);

		mf.run();
	}
	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = MultiFilters.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		System.out.println(sw.toString());
	}
	public void setArgs(int failCount, int threadCount, int target, int repeatCount, int partitionRange[],  Map<String, Boolean> wbool) {
		this.failCount = failCount;
		this.threadCount = threadCount;
		this.target = target;
		this.repeatCount = repeatCount;
		this.partitionRange = partitionRange;
		this.wbool = wbool;
	}

	public void run() throws AerospikeException {

		int[] cid = { 1, this.failCount };
		int activeCount = this.threadCount;
		int target = this.target/activeCount;
		final int PARTITIONS = 4096;
		final int CALWLEGS = 50;
		final int partitionsPerThread = (partitionRange[1]-partitionRange[0]+1)/activeCount;
		int pidStart;
		int pidCount;
		Future<Integer> future;   
		Callable<Integer> callable;
		List<Future<Integer>> resultList = new ArrayList<Future<Integer>>();

		AtomicInteger atomCount = new AtomicInteger(0);
		int repeat = this.repeatCount;
		long start = System.currentTimeMillis();

		ExecutorService es = Executors.newFixedThreadPool(activeCount);
		System.out.println("Thread pool size: " + activeCount);
		try {
			while (true) {
				repeat--;
				if (this.repeatCount > 1 && this.wbool.get("withDetail")) {
					System.out.printf("Repeat %d running:\n", this.repeatCount-repeat);
					System.out.println("---------------------------");
				}
				if (this.repeatCount > 1 && !this.wbool.get("withDetail")) {
					System.out.printf(".");
				}

				pidCount = partitionsPerThread;
				final int minPid = partitionRange[0];
				final int maxPid = partitionRange[1];
				if (repeat==0) {
					System.out.printf("\nScanning partitionId range: [%d, %d), CawlEg range: [%d, %d]\n\n", minPid, maxPid, cid[0], cid[1]);
				}
   	 		    for (int i = 0; i < activeCount; i++) {
					pidStart = i*partitionsPerThread+minPid;
					if (pidStart + 2*pidCount > maxPid) {
						pidCount = maxPid - pidStart;
					}
					int[] pid = {pidStart, pidCount};
					callable = new QueryTask(this.hosts, this.writePolicy, cid, pid, target, this.wbool);
					future = es.submit(callable);
					resultList.add(future);
					//es.execute(qt);
					//break;
				}
		
				for (Future<Integer> fs : resultList) {
					try{   
						atomCount.getAndAdd(fs.get());
					}catch(NullPointerException e) {
						e.printStackTrace();
					}catch(InterruptedException e) {
						e.printStackTrace();
					}catch(ExecutionException e) {
						e.printStackTrace();
					}finally {
						//es.shutdown();
					}
				}
				resultList.clear();
				if (repeat == 0) {
					break;
				}
			}
		}
		finally {
			es.shutdown();
		}

		int scanCount = atomCount.get();
		long interval = System.currentTimeMillis() - start;
		if (scanCount == 0) {
			System.out.println("No results returned.");			
		}else {
			int rc = this.repeatCount;
			System.out.printf("\nSummary:\n");
			System.out.printf("Total run time: %.2fs\n", interval/1000.0);			
			System.out.printf("Average scanning count: %d\n", scanCount/rc);
			System.out.printf("Average scanning time: %.2fs\n", interval/rc/1000.0);			
			System.out.printf("Total TPS: %.2f\n\n", 1000.0*scanCount/interval);			
		}
	}

	protected void finalize() throws Throwable {
	    if (this.client != null) {
	        this.client.close();
	        this.client = null;
	    }
	}
	protected void printInfo(String title, String infoString){
		String[] outerParts = infoString.split(";");
		System.out.println(title);
		for (String s : outerParts){

			String[] innerParts = s.split(":");
			for (String parts : innerParts){
				System.out.println("\t" + parts);
			}
			System.out.println();
		}
		
	}
}

class QueryTask implements Callable<Integer> {
	final AerospikeClient client;
	final WritePolicy writePolicy;
	private int cidStart;
	private int cidCount;
	private int pidStart;
	private int pidCount;
	private int target;
	private Map<String, Boolean> wbool;

	public QueryTask(Host[] hosts, WritePolicy writePolicy, int[] cid, int[] pid, int target, Map<String, Boolean> wbool) throws AerospikeException {
		this.client = new AerospikeClient(new ClientPolicy(), hosts);
		this.writePolicy = writePolicy;

		if (writePolicy == null) {
			writePolicy = new WritePolicy();
		}

		setArgs(cid, pid, target, wbool);
	}

	public void setArgs(int[] cid, int[] pid, int target, Map<String, Boolean> wbool) {
		this.cidStart = cid[0];
		this.cidCount = cid[1];
		this.pidStart = pid[0];
		this.pidCount = pid[1];
		this.target = target;
		this.wbool = wbool;
	}

	public void cleanup() {
		this.client.close();
	}

	public static String genRandomString(int length) {    
		StringBuffer buffer = new StringBuffer("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");    
		StringBuffer sb = new StringBuffer();    
		Random r = new Random();    
		int range = buffer.length();    
		for (int i = 0; i < length; i ++) {    
			sb.append(buffer.charAt(r.nextInt(range)));    
		}    
		return sb.toString();    
	}

	@Override
	public Integer call() throws Exception {
		//Record record = null;
		ResultSet resultSet = null;
		Statement stmt = null;	
		QueryTask queryTask = null;
		String namespace = "state";
		String setName = "demo";
		String regName = setName + "Reg";
		String regData = QueryTask.genRandomString(32000);

		final long TPS_CHECK_INTERVAL = 500;
		final long STATUS_REPORT_INTERVAL = 5000;
		final long SLEEP_INTERVAL = 1;

		if (this.wbool.get("withDetail")) {
			System.out.printf("Running QueryTask(partitionId[%d, %d))\n", this.pidStart, this.pidStart+this.pidCount);
		}
		// aggregate state_udf.multifilters(1) on state.demo where partitionId between 1 and 83
		stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(setName);
		stmt.setFilters(Filter.range("partitionId", Value.get(this.pidStart), Value.get(this.pidStart+this.pidCount-1)));
		//Value arr[] = { Value.get(1), Value.get(3) };
		long start = System.currentTimeMillis();

		try {
			resultSet = client.queryAggregate(null, stmt, 
				"state_udf", "multifilters",  Value.get(this.cidStart), Value.get(this.cidStart+this.cidCount));
		} catch (AerospikeException ae) {
			//ae.printStackTrace();
			System.out.println(ae);
		}
		int count = 0;
		int delta = 0;
		boolean success;
		Map<String, Integer> map;
		WritePolicy genPolicy = new WritePolicy();
		genPolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;

		long start_time = System.currentTimeMillis();
		long last_time1 = start_time;
		long last_time2 = start_time;
		long current_time;
		double _target = this.target;

		try {
			while (resultSet.next()) {
				success = true;
				Object object = resultSet.getObject();
				//System.out.println("Result: " + object);
				map = (Map<String, Integer>)object;
				try {
					/*
					if (this.wbool.get("withDetail")) {
						System.out.println("Skey: " + map.get("skey"));
					}
					*/
					// Write single value.
					if (this.wbool.get("withReassign")) {
						//genPolicy.generation =  map.get("gen").intValue();
						client.put(this.writePolicy, new Key(namespace, setName,  Value.get(map.get("skey"))),
								new Bin("cawlEgId", Value.get(map.get("cawlEgId"))));
					}
					// Load registry data
					if (this.wbool.get("withLoad")) {
						client.put(this.writePolicy, new Key(namespace, regName,  Value.get(map.get("skey"))),
								new Bin("skey", Value.get(map.get("skey"))),
								new Bin("regdata", Value.get(regData)));
					}
					// Restore registry data
					if (this.wbool.get("withRestore")) {
						client.get(this.writePolicy, new Key(namespace, regName,  Value.get(map.get("skey"))));
					}

				} catch (AerospikeException ae) {
					success = false;
					System.out.println(ae);
					//ae.printStackTrace();
				}
				if (success) {
					count++;
				}
				current_time = System.currentTimeMillis();
				if (_target != 0 && (current_time-last_time1) >= TPS_CHECK_INTERVAL) {
					while (_target < count/((double)System.currentTimeMillis()-start_time)*1000) {
						try {
							TimeUnit.MILLISECONDS.sleep(SLEEP_INTERVAL);
						} catch(InterruptedException e) {
							// do nothing
						}
					}
					last_time1 = current_time;
				}
				current_time = System.currentTimeMillis();
				if (this.wbool.get("withDetail") && (current_time-last_time2) >= STATUS_REPORT_INTERVAL) {
					System.out.printf("%d throughput: %.2f\n", delta, count/(double)(current_time-start_time)*1000);
					delta += 5;
					last_time2 = current_time;
				}
			}

			if (this.wbool.get("withDetail")) {
				System.out.printf("QueryTask(partitionId[%d, %d)) done\n", this.pidStart, this.pidStart+this.pidCount);
			}
			
			long interval = System.currentTimeMillis() - start;
			if (count == 0) {
				System.out.println("No results returned.");			
			}else if (this.wbool.get("withDetail")){
				System.out.printf("Scanning count: %d\n", count);			
				System.out.printf("Scaning time: %.2fs\n", interval/1000.0);			
				System.out.printf("TPS: %.2f\n\n", 1000.0*count/interval);			
			}
		}catch (AerospikeException ae) {
			//ae.printStackTrace();
			System.out.println(ae);
		}
		finally {
			resultSet.close();
			cleanup();
		}
		return count;
	}
}
