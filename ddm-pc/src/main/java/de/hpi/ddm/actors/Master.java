package de.hpi.ddm.actors;

import akka.actor.*;
import de.hpi.ddm.structures.PasswordCrackingJob;
import lombok.*;
import org.apache.commons.lang3.tuple.Pair;

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

    private final HashMap<Pair<String, Integer>, List<String>> hashStore = new HashMap<>();
    private final HashMap<String, PasswordCrackingJob> passwordCrackingJobMap = new HashMap<>();
    private final HashMap<String, Queue<Worker.CompareMessage>> tasks = new HashMap<>();
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

        getReader().tell(new Reader.ReadMessage(), self());
    }

    private void handle(BatchMessage message) {
        if (message.getLines().isEmpty()) {
            getCollector().tell(new Collector.PrintMessage(), self());
            terminate();
            return;
        }

        for (String[] line : message.getLines()) {
            PasswordCrackingJob passwordCrackingJob = parsePasswordCrackingJob(line);
            getPasswordCrackingJobs().add(passwordCrackingJob);
            getPasswordCrackingJobMap().put(passwordCrackingJob.getId(), passwordCrackingJob);
        }

        prepareNextPasswordCrackingJob();

        getWorkers().forEach(this::sendNextTaskToWorker);
    }

    private void handle(CompareResult message) {
        System.out.print(".");
        if (message.hasResult() && getPasswordCrackingJobMap().containsKey(message.getJobId())) {
            PasswordCrackingJob job = getPasswordCrackingJobMap().get(message.getJobId());
            if (job.hasUnresolvedHints()) {
                replaceHintHashes(job, message.getResolvedHashes());
                if (job.allHintsSolved()) {
                    applyHints(job);
                    getTasks().put(job.getId(), createTasks(job));
                }
                printJobStatus();
            } else if (message.getResolvedHashes().get(0).getHash().equals(job.getHash())) {
                job.setCrackedPassword(message.getResolvedHashes().get(0).getPlain());
                log().info("<Job " + job.getId() + "> PASSWORD " + job.getCrackedPassword());
                getTasks().remove(job.getId());
                getPasswordCrackingJobMap().remove(job.getId());
                sendSolvedPasswordsToCollector();
            }
        }

        if (getPasswordCrackingJobs().isEmpty())
            getReader().tell(new Reader.ReadMessage(), self());
        else if (getTasks().containsKey(message.getJobId()) && !getTasks().get(message.getJobId()).isEmpty()) {
            sendNextTaskToWorker(sender(), message.getJobId());
        } else {
            prepareNextPasswordCrackingJob();
            sendNextTaskToWorker(sender());
        }
    }

    private void prepareNextPasswordCrackingJob() {
        for (PasswordCrackingJob job : getPasswordCrackingJobs()) {
            if (!job.isStarted()) {
                job.setStarted(true);
                getTasks().put(job.getId(), createTasks(job));
                break;
            }
        }
        printJobStatus();
    }

    private void printJobStatus() {
        log().info("");
        log().info("====================================");
        getTasks().forEach((id, tasks) ->
                log().info("<Job " + id + "> Tasks remaining: " + tasks.size() + " Hints unresolved: " + getPasswordCrackingJobMap().get(id).getUnresolvedHintCount()));
        log().info("====================================");
    }

    private void sendSolvedPasswordsToCollector() {
        PasswordCrackingJob completedJob;
        while ((completedJob = getPasswordCrackingJobs().peek()) != null && completedJob.isSolved()) {
            getCollector().tell(new Collector.CollectMessage(getPasswordCrackingJobs().poll().getCrackedPassword()), self());
        }
    }

    private void applyHints(PasswordCrackingJob passwordCrackingJob) {
        Set<Character> missingCharacters = new HashSet<>();
        passwordCrackingJob.getHints().forEach(hint -> missingCharacters.addAll(
                passwordCrackingJob.getRemainingChars().stream()
                        .filter(c -> hint.indexOf(c) == -1)
                        .collect(Collectors.toSet())));
        passwordCrackingJob.getRemainingChars().removeAll(missingCharacters);
    }

    private void replaceHintHashes(PasswordCrackingJob job, List<CompareResult.Result> resolvedHashes) {
        for (int iHint = 0; iHint < job.getHints().size(); iHint++) {
            for (CompareResult.Result result : resolvedHashes) {
                if (job.getHints().get(iHint).equals(result.getHash())) {
                    job.getHints().set(iHint, result.getPlain());
                    job.decrementUnresolvedHintCount();
                    break;
                }
            }
        }
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

    private Queue<Worker.CompareMessage> createTasks(PasswordCrackingJob passwordCrackingJob) {
        String occurringCharacters = passwordCrackingJob.getRemainingCharsAsString();
        LinkedList<String> permutations = new LinkedList<>();
        permutation(occurringCharacters.toCharArray(), passwordCrackingJob.getPasswordLength(), permutations);

        passwordCrackingJob.setPermutations(permutations);
        int totalPermutations = permutations.size();

        int chunkCount = (int) Math.ceil((double) totalPermutations / getCHUNK_SIZE());
        Queue<Worker.CompareMessage> hintCrackingTasks = new LinkedList<>();
        for (int iChunk = 0; iChunk < chunkCount; iChunk++) {
            int offset = iChunk * getCHUNK_SIZE();
            hintCrackingTasks.add(new Worker.CompareMessage(
                    offset,
                    (iChunk + 1) * getCHUNK_SIZE() < totalPermutations ? getCHUNK_SIZE() : totalPermutations - offset,
                    null,
                    null,
                    null, // will be updated right before sending
                    passwordCrackingJob.getId()
            ));
        }
        return hintCrackingTasks;
    }

    private void sendNextTaskToWorker(ActorRef worker) {
        for (PasswordCrackingJob job : getPasswordCrackingJobs())
            if (getTasks().get(job.getId()).peek() != null) {
                sendNextTaskToWorker(worker, job.getId());
                return;
            }
    }

    private void sendNextTaskToWorker(ActorRef worker, String passwordCrackingJobId) {
        Worker.CompareMessage task = getTasks().get(passwordCrackingJobId).poll();
        if (task != null) {
            PasswordCrackingJob job = getPasswordCrackingJobMap().get(passwordCrackingJobId);
            task.setPermutations(new LinkedList<>(job.getPermutations().subList(task.getOffset(), task.getOffset() + task.getLength())));
            task.setOccurringCharacters(job.getRemainingCharsAsString());
            task.setHashes(new LinkedList<>(job.allHintsSolved() ? Collections.singletonList(job.getHash()) : job.getHints()));
            worker.tell(task, self());
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
