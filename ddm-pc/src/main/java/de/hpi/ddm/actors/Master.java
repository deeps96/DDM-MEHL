package de.hpi.ddm.actors;

import akka.actor.*;
import de.hpi.ddm.PermutationGenerator;
import de.hpi.ddm.structures.PasswordCrackingJob;
import lombok.*;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static de.hpi.ddm.Utils.*;

@Getter(AccessLevel.PRIVATE)
public class Master extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";
    private static boolean DEBUG = false;

    public static Props props(final ActorRef reader, final ActorRef collector) {
        return Props.create(Master.class, () -> new Master(reader, collector));
    }

    private Master(final ActorRef reader, final ActorRef collector) {
        this.reader = reader;
        this.collector = collector;
        this.workers = new ArrayList<>();
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    public static class StartMessage implements Serializable {
        private static final long serialVersionUID = -50374816448627600L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<String[]> lines;
    }

    @Data
    static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
    }

    @Data @AllArgsConstructor @NoArgsConstructor
    static class CompareResult implements Serializable {
        private static final long serialVersionUID = 1294419813760526676L;
        private LinkedList<Result> resolvedHashes;
        private String jobId;

        boolean hasResult() {
            return !getResolvedHashes().isEmpty();
        }

        @Data @AllArgsConstructor @NoArgsConstructor
        static class Result implements Serializable {
            private static final long serialVersionUID = 649337839499917549L;
            String hash;
            String plain;
        }
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef reader;
    private final ActorRef collector;
    private final List<ActorRef> workers;

    private long startTime;

    @Setter(AccessLevel.PRIVATE)
    private boolean isFetchingNextBatch = false;
    private final HashMap<String, PasswordCrackingJob> passwordCrackingJobMap = new HashMap<>();
    private final Queue<PasswordCrackingJob> passwordCrackingJobs = new LinkedList<>();

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::handle)
                .match(BatchMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .match(RegistrationMessage.class, this::handle)
                .match(CompareResult.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();

        getPasswordCrackingJobs().clear();

        // getReader().tell(new Reader.ReadMessage(), self());
    }

    private void handle(BatchMessage message) {
        if (message.getLines().isEmpty()) {
            getCollector().tell(new Collector.PrintMessage(), self());
            terminate();
            return;
        }

        setFetchingNextBatch(false);

        for (String[] line : message.getLines()) {
            PasswordCrackingJob passwordCrackingJob = parsePasswordCrackingJob(line);
            getPasswordCrackingJobs().add(passwordCrackingJob);
            getPasswordCrackingJobMap().put(passwordCrackingJob.getId(), passwordCrackingJob);
        }

        getWorkers().forEach(this::sendNextTaskToWorker); // we might serve tasks to already busy workers here, since we assign task on registration
    }

    private void handle(CompareResult message) {
        if(DEBUG)
            System.out.print(".");

        if (message.hasResult() && getPasswordCrackingJobMap().containsKey(message.getJobId())) {
            PasswordCrackingJob job = getPasswordCrackingJobMap().get(message.getJobId());

            if (job.hasUnresolvedHints()) {
                replaceHintHashes(job, message.getResolvedHashes());

                if (job.readyToCrackPassword()) {
                    createPasswordCrackingTasks(job);
                }

                if(DEBUG)
                    printJobStatus();
            } else if (message.getResolvedHashes().get(0).getHash().equals(job.getHash())) {
                job.setCrackedPassword(message.getResolvedHashes().get(0).getPlain());
                if(DEBUG)
                    log().info("<Job " + job.getId() + "> PASSWORD " + job.getCrackedPassword());
                getPasswordCrackingJobMap().remove(job.getId());
                sendSolvedPasswordsToCollector();
            }
        }

        if (getPasswordCrackingJobMap().containsKey(message.getJobId()) &&
                !getPasswordCrackingJobMap().get(message.getJobId()).getPermutationGenerator().isDone())
            sendNextTaskToWorker(sender(), message.getJobId());
        else
            sendNextTaskToWorker(sender());
    }

    private void prepareNextPasswordCrackingJob() {
        for (PasswordCrackingJob job : getPasswordCrackingJobs()) {
            if (!job.isStarted()) {
                job.setStarted(true);
                createHintCrackingTasks(job);
                if(DEBUG)
                    printJobStatus();
                return;
            }
        }

        if (!isFetchingNextBatch()) {
            setFetchingNextBatch(true);
            getReader().tell(new Reader.ReadMessage(), self());
        }
    }

    private void printJobStatus() {
        log().info("");
        log().info("====================================");
        getPasswordCrackingJobMap().forEach((id, job) -> {
            if (job.isStarted() && !job.isSolved())
                log().info("<Job " + id + "> Ready to crack password: " + job.readyToCrackPassword() + " Hints solved: " + (job.getHints().size() - job.getUnresolvedHintCount()) + "/" +  job.getHints().size());
        });
        log().info("Jobs remaining: " + getPasswordCrackingJobs().size());
        log().info("====================================");
    }

    private void sendSolvedPasswordsToCollector() {
        PasswordCrackingJob completedJob;
        while ((completedJob = getPasswordCrackingJobs().peek()) != null && completedJob.isSolved()) {
            getCollector().tell(new Collector.CollectMessage(getPasswordCrackingJobs().poll().getCrackedPassword()), self());
        }
    }

    private void replaceHintHashes(PasswordCrackingJob job, List<CompareResult.Result> resolvedHashes) {
        Set<Character> missingCharacters = new HashSet<>();

        for (int iHint = 0; iHint < job.getHints().size(); iHint++) {
            for (CompareResult.Result result : resolvedHashes) {
                if (job.getHints().get(iHint).equals(result.getHash())) {
                    job.getHints().set(iHint, result.getPlain());
                    job.decrementUnresolvedHintCount();
                    missingCharacters.addAll(
                            job.getRemainingChars().stream()
                                    .filter(c -> result.getPlain().indexOf(c) == -1)
                                    .collect(Collectors.toSet()));
                    break;
                }
            }
        }

        job.getRemainingChars().removeAll(missingCharacters);
    }

    private final int CSV_PASSWORD_CHARS_COLUMN_INDEX = 2;
    private final int CSV_PASSWORD_LENGTH_COLUMN_INDEX = 3;
    private final int CSV_PASSWORD_COLUMN_INDEX = 4;
    private final int CSV_HINT_START_COLUMN_INDEX = 5;

    private PasswordCrackingJob parsePasswordCrackingJob(String[] line) {
        return new PasswordCrackingJob(
                UUID.randomUUID(),
                line[getCSV_PASSWORD_CHARS_COLUMN_INDEX()],
                line[getCSV_PASSWORD_COLUMN_INDEX()],
                new LinkedList<>(Arrays.asList(Arrays.copyOfRange(line, getCSV_HINT_START_COLUMN_INDEX(), line.length))),
                Integer.parseInt(line[getCSV_PASSWORD_LENGTH_COLUMN_INDEX()])
        );
    }

    private final int CHUNK_SIZE = 16_384;

    private void createPasswordCrackingTasks(PasswordCrackingJob passwordCrackingJob) {
        String occurringCharacters = passwordCrackingJob.getRemainingCharsAsString();
        PermutationGenerator permutationGenerator = permutationsForPasswordCracking(occurringCharacters.toCharArray(), passwordCrackingJob.getPasswordLength());

        passwordCrackingJob.setPermutationGenerator(permutationGenerator);
    }

    private void createHintCrackingTasks(PasswordCrackingJob passwordCrackingJob) {
        String occurringCharacters = passwordCrackingJob.getRemainingCharsAsString();
        PermutationGenerator generator = permutationsForHintCracking(occurringCharacters.toCharArray());
        passwordCrackingJob.setPermutationGenerator(generator);
    }

    private void sendNextTaskToWorker(ActorRef worker) {
        PasswordCrackingJob runningJob = getRunningPasswordCrackingJob();
        if (runningJob == null) {
            prepareNextPasswordCrackingJob();
            runningJob = getRunningPasswordCrackingJob();
        }

        if (runningJob != null) {
            sendNextTaskToWorker(worker, runningJob.getId());
        }
    }

    private PasswordCrackingJob getRunningPasswordCrackingJob() {
        for (PasswordCrackingJob job : getPasswordCrackingJobs()) {
            if (job.isStarted() && !job.isSolved() && job.getPermutationGenerator() != null && !job.getPermutationGenerator().isDone())
                return job;
        }
        return null;
    }

    private void sendNextTaskToWorker(ActorRef worker, String passwordCrackingJobId) {
        PasswordCrackingJob job = getPasswordCrackingJobMap().get(passwordCrackingJobId);

        if (job.getPermutationGenerator() != null && !job.getPermutationGenerator().isDone()) {
            Worker.CompareMessage task = new Worker.CompareMessage(
                    new LinkedList<>(job.readyToCrackPassword() ? Collections.singletonList(job.getHash()) : job.getHints()),
                    new LinkedList<>(job.getPermutationGenerator().getNextBatch(getCHUNK_SIZE())),
                    passwordCrackingJobId);
            if (task.getPermutations().isEmpty()) { // no permutations left
                sendNextTaskToWorker(worker);
            } else {
                worker.tell(task, self());
            }
        }
    }

    private void handle(RegistrationMessage message) {
        context().watch(sender());
        getWorkers().add(sender());
        sendNextTaskToWorker(sender());
    }

    private void handle(Terminated message) {
        this.context().unwatch(message.getActor());
        this.workers.remove(message.getActor());
    }

    private void terminate() {
        this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
        this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());

        for (ActorRef worker : this.workers) {
            this.context().unwatch(worker);
            worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }

        this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());

        long executionTime = System.currentTimeMillis() - this.startTime;
        this.log().info("Algorithm finished in {} ms", executionTime);
    }
}
