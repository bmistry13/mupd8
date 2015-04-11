package com.walmartlabs.mupd8;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Mupd8StatusAndShutdownUtill {

	private static final String sysProperty = "mupd8.shutdown.wait.in.ms";
	private static final Logger LOG = LoggerFactory.getLogger(Mupd8StatusAndShutdownUtill.class);

	// Defalt wait 2 minutes..
	private static long TIME_TO_WAIT_IN_MS = TimeUnit.MINUTES.toMillis(2L);;

	private static boolean shutdownExecuted;
	private static Date shutdownTime;

	private static final List<Mupd8Shutdown> shutdownHooks = Collections
			.synchronizedList(new ArrayList<Mupd8Shutdown>(15));

	private static final List<Mupd8SourceStats> stats = Collections
			.synchronizedList(new ArrayList<Mupd8SourceStats>(15));

	private static final Set<String> supportedURL = new HashSet<String>();

	static {
		
		supportedURL.add("shutdown");
		supportedURL.add("threaddump");
		supportedURL.add("stats");
		String value = System.getProperty(sysProperty);
		if (value != null) {
			try {
				TIME_TO_WAIT_IN_MS = Long.parseLong(value.trim());
			} catch (NumberFormatException exp) {
				LOG.error("mupd8.shutdown.wait.in.ms value is :" + value
						+ " is invalid...using default value "
						+ TIME_TO_WAIT_IN_MS);
			}
		}
		Runtime.getRuntime().addShutdownHook(new Thread("MUPD-SHUTDOWN-HOOK") {
			@Override
			public void run() {
				shutdownNow();
			}
		});
	}

	public static final synchronized String getSystemThreadDump() {

		StringBuilder builder = new StringBuilder(15 * 1024);

		ThreadInfo[] threads = ManagementFactory.getThreadMXBean()
				.dumpAllThreads(true, true);
		for (final ThreadInfo info : threads) {
			builder.append(info.toString());
		}
		// FIND DEADLOCKED THREADS
		builder.append('\n');
		builder.append('\n');
		builder.append("findMonitorDeadlockedThreads\n");
		long[] thIds = ManagementFactory.getThreadMXBean()
				.findMonitorDeadlockedThreads();
		if (thIds != null) {
			builder.append(Arrays.toString(thIds));
		}
		builder.append('\n');
		
		return builder.toString();
	}

	public static synchronized void registerShutdown(final Mupd8Shutdown hook) {
		StackTraceElement[] stackTraceElements = Thread.currentThread()
				.getStackTrace();
		String callingClass = Mupd8StatusAndShutdownUtill.class.getCanonicalName();
		if (stackTraceElements.length > 2) {
			// this should be most of the case
			callingClass = stackTraceElements[2].getClassName();
		} else if (stackTraceElements.length > 1) {
			callingClass = stackTraceElements[1].getClassName();
		} else {
			callingClass = stackTraceElements[0].getClassName();
		}
		LOG.info("Adding Shutdown hook from class=" + callingClass);
		shutdownHooks.add(new Mupd8ShutdownWrapper(hook, callingClass));
	}

	public static synchronized void registerStats(final Mupd8SourceStats stat) {
		StackTraceElement[] stackTraceElements = Thread.currentThread()
				.getStackTrace();
		String callingClass = Mupd8StatusAndShutdownUtill.class.getCanonicalName();
		if (stackTraceElements.length > 2) {
			// this should be most of the case
			callingClass = stackTraceElements[2].getClassName();
		} else if (stackTraceElements.length > 1) {
			callingClass = stackTraceElements[1].getClassName();
		} else {
			callingClass = stackTraceElements[0].getClassName();
		}
		LOG.info("Adding Stats from class=" + callingClass);
		stats.add(stat);
	}

	private static class Mupd8ShutdownWrapper implements Mupd8Shutdown {
		Mupd8Shutdown target;
		String callingClazz;

		public Mupd8ShutdownWrapper(Mupd8Shutdown target, String callingClazz) {
			this.target = target;
			this.callingClazz = callingClazz;
		}

		@Override
		public void shutdown() {
			target.shutdown();
		}

		@Override
		public String toString() {
			return "Registered Class: " + callingClazz + " " + super.toString();
		}
	}

	private static void shutdownNow() {
		for (Mupd8Shutdown hook : shutdownHooks) {
			try {
				LOG.info("Executing Shutdown Hooks" + hook.toString());
				hook.shutdown();
			} catch (Throwable th) {
				LOG.error("Shutdown hook error " + hook.toString(), th);
			}
		}
	}

	public static String handleHttpRequest(String url) {
		StringBuilder body = new StringBuilder(5 * 1024);
		if (url != null) {
			url = url.toLowerCase();
			if (url.contains("shutdown")) {
				if (shutdownExecuted) {
					body.append(
							"Shutdown Executed.... already in progress.....it will shutdown at ")
							.append(shutdownTime.toString());
				} else {
					synchronized (Mupd8StatusAndShutdownUtill.class) {
						if (!shutdownExecuted) {
							shutdownExecuted = true;
							// / shutdown..for...now..
							shutdownNow();
							shutdownTime = new Date(System.currentTimeMillis()
									+ TIME_TO_WAIT_IN_MS);
							body.append(
									"Shutdown executed for registered hooks size=")
									.append(shutdownHooks.size())
									.append(". Please wait 2 minutes from now to auto shutdown..at "
											+ shutdownTime.toString());
							Thread th = new Thread("Mupd8-Web-Shutdown-Hook") {
								@Override
								public void run() {
									try {
										Thread.sleep(TIME_TO_WAIT_IN_MS);
									} catch (InterruptedException e) {
										e.printStackTrace();
										return;
									}
									LOG.info("Exiting JVM Now " + new Date());
									// EXIT JVM THIS IS ONLY WAY RIGHT NOW and hopefully in this two minutes all events are flushed..MUPD Lib needs to have graceful Shutdown..
									System.exit(0);
								}
							};
							th.start();
						} else {
							body.append("Shutdown Executed.... already in progress.....it will shutdown at ")
									.append(shutdownTime.toString());
						}
					}
				}
			} else if (url.contains("stats")) {
				
				if(stats.size() > 0){
					for (Mupd8SourceStats statGen : stats) {
						String data = statGen.generateStats();
						if (data != null) {
							body.append(data);
						}
					}
				}else{
					body.append("No Stats Registered...!");
				}			
				if(body.length() == 0){
					body.append("Stats Registered Size = ").append(stats.size()).append(". But No stats output.");
				}
			} else if (url.contains("threaddump")) {
				return getSystemThreadDump();
			}

		}
		if (body.length() == 0) {
			return "NO DATA\n";
		} else {
			return body.append('\n').toString();
		}
	}
	
	public static boolean isSupportedEndPoint(String token){
		if(token != null){
			return supportedURL.contains(token.trim().toLowerCase());
		}else{
			return false;
		}
	}
}
