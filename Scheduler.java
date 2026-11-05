import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;

/**
 * Real-time job scheduling simulator (next-event simulation).
 * Simulates RM, DM and EDF (preemptive) on the same set of periodic processes.
 *
 * Usage:  java Scheduler [--horizon N] input1.txt [input2.txt ...]
 * Results are printed and also saved to results.txt.
 */
public class Scheduler {

    enum Policy { RM, DM, EDF }

    /** Event types. Their order also decides which event is handled first at the same time. */
    enum EventType { COMPLETION, DEADLINE, ARRIVAL }

    /** One periodic process read from the input file. */
    static class Process {
        int id, arrival, deadline, period, service, order;
        boolean serviceGiven;
        List<Integer> finishes = new ArrayList<>();
        List<Integer> missedAt = new ArrayList<>();
    }

    /** One instance (job) of a process, released once per period. */
    static class Job {
        Process p;
        int release, absDeadline, remaining;
        boolean done;
    }

    /** An entry in the event queue. token is used to ignore outdated completion events after preemption. */
    static class Event {
        int time, token;
        long seq;
        EventType type;
        Job job;
    }

    /** A piece of the CPU timeline: process pid ran from start to end. */
    static class Segment {
        int start, end, pid;
        Segment(int s, int e, int p) { start = s; end = e; pid = p; }
    }

    static final StringBuilder out = new StringBuilder();

    static void log(String s) {
        System.out.println(s);
        out.append(s).append(System.lineSeparator());
    }

    // ---------------- Simulation state (reset for every run) ----------------
    private final Policy policy;
    private final int horizon;
    private final int switchTime;
    private final List<Process> procs;

    private final PriorityQueue<Event> events = new PriorityQueue<>(
            Comparator.comparingInt((Event e) -> e.time)
                      .thenComparingInt(e -> e.type.ordinal())
                      .thenComparingLong(e -> e.seq));
    private final List<Job> ready = new ArrayList<>();
    private final List<Segment> timeline = new ArrayList<>();
    private long seqCounter = 0;
    private Job running = null, lastRun = null, segJob = null;
    private int runStart = 0, segStart = 0, runToken = 0;
    private int busyTime = 0, switches = 0, missCount = 0;
    private int firstMissTime = -1, firstMissPid = -1;

    Scheduler(Policy policy, List<Process> procs, int switchTime, int horizon) {
        this.policy = policy;
        this.procs = procs;
        this.switchTime = switchTime;
        this.horizon = horizon;
        for (Process p : procs) {
            p.finishes.clear();
            p.missedAt.clear();
        }
    }

    private void addEvent(int time, EventType type, Job job, int token) {
        Event e = new Event();
        e.time = time;
        e.type = type;
        e.job = job;
        e.token = token;
        e.seq = seqCounter++;
        events.add(e);
    }

    /** Static priority key for RM/DM, absolute deadline for EDF. Smaller = higher priority. */
    private int key(Job j) {
        switch (policy) {
            case RM:  return j.p.period;
            case DM:  return j.p.deadline;
            default:  return j.absDeadline;
        }
    }

    private final Comparator<Job> priority = Comparator.comparingInt((Job j) -> key(j))
            .thenComparingInt(j -> j.release)
            .thenComparingInt(j -> j.p.order);

    /** Runs the simulation: take the next event, move the clock, handle it, dispatch. */
    void run() {
        for (Process p : procs) {
            if (p.arrival < horizon) addEvent(p.arrival, EventType.ARRIVAL, newJob(p, p.arrival), 0);
        }
        while (!events.isEmpty() && events.peek().time <= horizon) {
            int now = events.peek().time;
            advanceClock(now);
            while (!events.isEmpty() && events.peek().time == now) handle(events.poll(), now);
            dispatch(now);
        }
        advanceClock(horizon);
        closeSegment(horizon);
    }

    private Job newJob(Process p, int release) {
        Job j = new Job();
        j.p = p;
        j.release = release;
        j.absDeadline = release + p.deadline;
        j.remaining = p.service;
        return j;
    }

    /** Charges the running job for the CPU time used since it was last dispatched. */
    private void advanceClock(int now) {
        if (running != null) {
            int used = now - runStart;
            running.remaining -= used;
            busyTime += used;
        }
        runStart = now;
    }

    private void handle(Event e, int now) {
        Job j = e.job;
        switch (e.type) {
            case ARRIVAL:
                ready.add(j);
                addEvent(j.absDeadline, EventType.DEADLINE, j, 0);
                int next = now + j.p.period;
                if (next < horizon) addEvent(next, EventType.ARRIVAL, newJob(j.p, next), 0);
                break;

            case COMPLETION:
                if (e.token != runToken || running != j) break;   // outdated (job was preempted)
                j.done = true;
                j.remaining = 0;
                j.p.finishes.add(now);
                ready.remove(j);
                running = null;
                break;

            case DEADLINE:
                if (j.done || !ready.contains(j)) break;
                // Deadline reached and job not finished: deadline miss, the job is aborted.
                j.p.missedAt.add(now);
                missCount++;
                if (firstMissTime < 0) {
                    firstMissTime = now;
                    firstMissPid = j.p.id;
                }
                ready.remove(j);
                if (running == j) running = null;
                break;
        }
    }

    /** Picks the highest-priority ready job; preempts the running job if a higher-priority job is ready. */
    private void dispatch(int now) {
        Job best = null;
        for (Job j : ready) if (best == null || priority.compare(j, best) < 0) best = j;

        if (running != null && best != null && key(best) >= key(running)) best = running; // no preemption on ties
        if (best != running) {
            running = best;
            if (best != null) {
                if (lastRun != null && lastRun != best) {
                    switches++;
                    best.remaining += switchTime;   // process switch overhead
                }
                lastRun = best;
                runStart = now;
                runToken++;
                addEvent(now + best.remaining, EventType.COMPLETION, best, runToken);
            }
        }
        if (running != segJob) {
            closeSegment(now);
            segJob = running;
            segStart = now;
        }
    }

    private void closeSegment(int now) {
        if (segJob != null && now > segStart) {
            Segment last = timeline.isEmpty() ? null : timeline.get(timeline.size() - 1);
            if (last != null && last.pid == segJob.p.id && last.end == segStart) last.end = now;
            else timeline.add(new Segment(segStart, now, segJob.p.id));
        }
    }

    // ---------------- Output ----------------

    void report() {
        log("");
        log(policy + " Results (simulated from 0 to " + horizon + " units):");
        if (firstMissTime < 0) {
            log("  Schedule feasible from 0 to " + horizon + " units. All deadlines met.");
        } else {
            log("  Schedule feasible from 0 to " + firstMissTime + " units. At " + firstMissTime
                    + ", process " + firstMissPid + " did not meet deadline.");
            log("  Total deadline misses in 0.." + horizon + ": " + missCount);
        }
        log(String.format(Locale.US, "  CPU time took %d units (busy) out of %d -> CPU load %.2f%%, idle %d units, process switches %d.",
                busyTime, horizon, 100.0 * busyTime / horizon, horizon - busyTime, switches));
        log("  " + analysis());
        for (Process p : procs) {
            StringBuilder sb = new StringBuilder();
            sb.append("  Process ").append(p.id)
              .append(": Arrival Time: ").append(p.arrival)
              .append(", Service: ").append(p.service)
              .append(", Deadline: ").append(p.deadline)
              .append(", Period ").append(p.period)
              .append(", finish: ").append(p.finishes.isEmpty() ? "none" : join(p.finishes)).append('.');
            if (!p.missedAt.isEmpty()) sb.append(" MISSED deadline at: ").append(join(p.missedAt)).append('.');
            log(sb.toString());
        }
        StringBuilder tl = new StringBuilder("  Timeline: ");
        int t = 0;
        for (Segment s : timeline) {
            if (s.start > t) tl.append("[").append(t).append("-").append(s.start).append(" idle] ");
            tl.append("[").append(s.start).append("-").append(s.end).append(" P").append(s.pid).append("] ");
            t = s.end;
        }
        if (t < horizon) tl.append("[").append(t).append("-").append(horizon).append(" idle]");
        log(wrap(tl.toString().stripTrailing(), 110, "            "));
    }

    /** Theoretical schedulability test for the policy, to compare with the simulated result. */
    private String analysis() {
        int n = procs.size();
        double u = 0, density = 0;
        for (Process p : procs) {
            u += (double) p.service / p.period;
            density += (double) p.service / Math.min(p.deadline, p.period);
        }
        double bound = n * (Math.pow(2, 1.0 / n) - 1);
        switch (policy) {
            case RM:
                return String.format(Locale.US, "Analysis: U = sum(C/T) = %.3f, RM bound n(2^(1/n)-1) = %.3f -> %s",
                        u, bound, u > 1 ? "U > 1, cannot be scheduled"
                                : u <= bound ? "guaranteed schedulable" : "not guaranteed by the bound (simulation decides)");
            case DM:
                return String.format(Locale.US, "Analysis: sum(C/D) = %.3f, bound = %.3f -> %s",
                        density, bound, u > 1 ? "U > 1, cannot be scheduled"
                                : density <= bound ? "guaranteed schedulable" : "not guaranteed by the bound (simulation decides)");
            default:
                return String.format(Locale.US, "Analysis: U = %.3f, density sum(C/min(D,T)) = %.3f -> %s",
                        u, density, u > 1 ? "U > 1, cannot be scheduled"
                                : density <= 1 ? "guaranteed schedulable" : "not guaranteed by density test (simulation decides)");
        }
    }

    String summaryRow() {
        return String.format(Locale.US, "  %-4s | %-8s | %-22s | %6d | %4d/%-4d | %6.2f%%",
                policy, firstMissTime < 0 ? "YES" : "NO",
                firstMissTime < 0 ? "-" : "t=" + firstMissTime + " (P" + firstMissPid + ")",
                missCount, busyTime, horizon, 100.0 * busyTime / horizon);
    }

    static String join(List<Integer> list) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) sb.append(i == 0 ? "" : ", ").append(list.get(i));
        return sb.toString();
    }

    static String wrap(String text, int width, String indent) {
        StringBuilder sb = new StringBuilder();
        int lineLen = 0;
        for (String word : text.split(" (?=\\[)")) {
            if (lineLen > 0 && lineLen + word.length() + 1 > width) {
                sb.append(System.lineSeparator()).append(indent);
                lineLen = indent.length();
            } else if (lineLen > 0) {
                sb.append(' ');
                lineLen++;
            }
            sb.append(word);
            lineLen += word.length();
        }
        return sb.toString();
    }

    // ---------------- Input ----------------

    /** Header values of the last file read: number of processes, process switch time. */
    static int[] header = new int[2];

    /**
     * Reads an input file. Header: "numberOfProcesses processSwitch".
     * Detail: "processNumber arrivalTime relativeDeadline period [serviceTime]".
     * Blank lines and lines starting with # are ignored.
     */
    static List<Process> readInput(String file) throws IOException {
        List<String> lines = new ArrayList<>();
        for (String line : Files.readAllLines(Paths.get(file))) {
            line = line.trim();
            if (!line.isEmpty() && !line.startsWith("#")) lines.add(line);
        }
        if (lines.isEmpty()) throw new IllegalArgumentException("empty input file");
        String[] h = lines.get(0).split("\\s+");
        int n = Integer.parseInt(h[0]);
        header[0] = n;
        header[1] = h.length > 1 ? Integer.parseInt(h[1]) : 0;
        if (lines.size() - 1 < n) throw new IllegalArgumentException("header says " + n + " processes but only " + (lines.size() - 1) + " found");

        List<Process> list = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            String[] f = lines.get(i).split("\\s+");
            if (f.length < 4) throw new IllegalArgumentException("line " + (i + 1) + ": expected at least 4 numbers");
            Process p = new Process();
            p.id = Integer.parseInt(f[0]);
            p.arrival = Integer.parseInt(f[1]);
            p.deadline = Integer.parseInt(f[2]);
            p.period = Integer.parseInt(f[3]);
            p.serviceGiven = f.length >= 5;
            p.service = p.serviceGiven ? Integer.parseInt(f[4]) : Math.max(1, p.deadline / 4);
            p.order = i;
            if (p.arrival < 0 || p.deadline <= 0 || p.period <= 0 || p.service <= 0)
                throw new IllegalArgumentException("line " + (i + 1) + ": values must be positive");
            list.add(p);
        }
        return list;
    }

    // ---------------- Main ----------------

    public static void main(String[] args) throws IOException {
        int forcedHorizon = -1;
        List<String> files = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--horizon") && i + 1 < args.length) forcedHorizon = Integer.parseInt(args[++i]);
            else files.add(args[i]);
        }
        if (files.isEmpty()) files.add("input.txt");

        for (String file : files) {
            List<Process> procs;
            try {
                procs = readInput(file);
            } catch (Exception e) {
                log("Could not read " + file + ": " + e.getMessage());
                continue;
            }
            log("=".repeat(110));
            log("INPUT FILE: " + file + "   (processes: " + header[0] + ", process switch: " + header[1] + ")");
            log("=".repeat(110));
            for (Process p : procs) {
                log(String.format("  P%d  arrival=%d  deadline=%d  period=%d  service=%d%s", p.id, p.arrival,
                        p.deadline, p.period, p.service, p.serviceGiven ? "" : "  (not in file, default = deadline/4)"));
            }

            // Default simulation lengths suggested by the assignment.
            int[] horizons = {101, 413, 500};
            List<String> rows = new ArrayList<>();
            for (Policy pol : Policy.values()) {
                int h = forcedHorizon > 0 ? forcedHorizon : horizons[pol.ordinal()];
                Scheduler sim = new Scheduler(pol, procs, header[1], h);
                sim.run();
                sim.report();
                rows.add(sim.summaryRow());
            }
            log("");
            log("Summary for " + file + ":");
            log("  Algo | Feasible | First deadline miss    | Misses | CPU busy  | CPU load");
            for (String r : rows) log(r);
            log("");
        }
        Path outFile = Paths.get("results.txt");
        Files.writeString(outFile, out.toString());
        System.out.println("Results saved to " + outFile.toAbsolutePath());
    }
}