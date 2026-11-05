# Real-Time-Scheduling-Simulator
Computer Science 305: Operating Systems project 3
# Real-Time Job Scheduling Simulator (RM, DM, EDF)

This is an event-driven (next-event) simulator, written in Java, for three preemptive real-time scheduling policies:

- **RM** (Rate Monotonic): the process with the shorter period gets higher priority.
- **DM** (Deadline Monotonic): the process with the shorter relative deadline gets higher priority.
- **EDF** (Earliest Deadline First): the job with the earliest absolute deadline runs first.

## How it works

- An **event queue**, sorted by time, holds three kinds of events: `ARRIVAL` (a new job of a process is released), `COMPLETION` (the running job finishes) and `DEADLINE` (checks whether a job met its deadline).
- The clock jumps straight to the next event. It handles every event at that time, then picks the highest-priority ready job, which may preempt the running one. New future events are added to the queue as it goes.
- If a job is still unfinished at its deadline, that counts as a **deadline miss**. The job is aborted and the simulation continues.
- The process switch time from the input header is added each time the CPU switches to a different job. It is 0 in all the input files.
- The simulation lengths are the ones the assignment suggests: **RM 0–101, DM 0–413, EDF 0–500**. Use `--horizon N` to set one length for all three.

## Input format

```
<number of processes> <process switch>
<process number> <arrival time> <relative deadline> <period> <service time>
...
```

The assignment's format has no service (execution) time, so the program accepts it as an optional **5th column**. If it is missing, as in `input-sample.txt`, the program uses `deadline / 4` and says so in the output. All processes in the supplied files arrive at time 0.

## Run

From the repo folder:

```bash
javac Scheduler.java
java Scheduler input-sample.txt input-1.txt input-2.txt input-3.txt input-4.txt
```

The results are printed and also saved to `results.txt`.

## Output (for each algorithm)

- Whether the schedule is feasible, and if not, when and which process first missed its deadline
- CPU busy time, CPU load % and idle time
- The theoretical schedulability test (utilization or bound) to compare against the simulation
- For each process: arrival, service, deadline, period, every finish time, and any missed deadlines
- The full execution timeline
- A summary table comparing RM, DM and EDF

## Test scenarios

| File | Scenario | RM | DM | EDF |
|---|---|---|---|---|
| `input-sample.txt` | The assignment's sample (default service times) | OK | OK | OK |
| `input-1.txt` | Light load (U = 0.63) | OK | OK | OK |
| `input-2.txt` | Heavy load (U = 0.95), deadline = period | Fails at t=20 | Fails at t=20 | OK |
| `input-3.txt` | Overload (U = 1.10) | Fails | Fails | Fails |
| `input-4.txt` | Short deadline on a longer-period process | Fails at t=2 | OK | OK |

What the tests show: EDF can use the CPU up to 100% when deadline = period. RM and DM can fail above their bounds. DM beats RM when a process's deadline is shorter than its period. No policy can schedule a load above 100%.