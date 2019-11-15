package de.hpi.ddm.actors;

import akka.actor.*;
import de.hpi.ddm.Utils;
import de.hpi.ddm.structures.PasswordCrackingJob;
import lombok.*;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static de.hpi.ddm.Utils.heapPermutation;

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
    static class StoreHashesMessage implements Serializable {
        private static final long serialVersionUID = -4715813113760725017L;

        private int offset;
        private int permutationLength;
        private String occurringCharacters;
        private LinkedList<String> hashes;
    }

    @Data @AllArgsConstructor @NoArgsConstructor
    static class CompareResult implements Serializable {
        private static final long serialVersionUID = 1294419813760526676L;
        private LinkedList<Pair<String, String>> resolvedHashes;
        private String jobId;

        boolean hasResult() {
            return !getResolvedHashes().isEmpty();
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
                .match(StoreHashesMessage.class, this::handle)
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
            getTasks().put(passwordCrackingJob.getId(), createHintCrackingTasks(passwordCrackingJob));
            break; // debug <- delete this line
        }
        getWorkers().forEach(this::sendNextTaskToWorker);
    }

    private void handle(StoreHashesMessage message) {
        List<String> hashes = getHashStore().get(Pair.of(message.getOccurringCharacters(), message.getPermutationLength()));
        for (int iHash = 0; iHash < message.getHashes().size(); iHash++)
            hashes.set(message.getOffset() + iHash, message.getHashes().get(iHash));
    }

    private void handle(CompareResult message) {
        if (message.hasResult() && getPasswordCrackingJobMap().containsKey(message.getJobId())) {
            PasswordCrackingJob job = getPasswordCrackingJobMap().get(message.getJobId());
            if (job.hasUnresolvedHints()) {
                replaceHintHashes(job, message.getResolvedHashes());
                if (job.allHintsSolved())
                    getTasks().put(job.getId(), createPasswordCrackingTasks(job));
            } else {
                job.setCrackedPassword(message.getResolvedHashes().get(0).getRight());
                log().info(job.getCrackedPassword());
                getTasks().remove(job.getId());
                getPasswordCrackingJobMap().remove(job.getId());
                sendSolvedPasswordsToCollector();
            }
            printJobStatus();
        }

        System.out.print(".");

        if (getPasswordCrackingJobs().isEmpty())
            getReader().tell(new Reader.ReadMessage(), self());
        else if (getTasks().containsKey(message.getJobId()) && !getTasks().get(message.getJobId()).isEmpty())
            sendNextTaskToWorker(sender(), message.getJobId());
        else
            sendNextTaskToWorker(sender());
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

    private Queue<Worker.CompareMessage> createPasswordCrackingTasks(PasswordCrackingJob passwordCrackingJob) {
        applyHints(passwordCrackingJob);
        return createTasks(
                passwordCrackingJob,
                new LinkedList<>(Collections.singletonList(passwordCrackingJob.getHash())),
                passwordCrackingJob.getPasswordLength());
    }

    private void applyHints(PasswordCrackingJob passwordCrackingJob) {
        Set<Character> missingCharacters = new HashSet<>();
        passwordCrackingJob.getHints().forEach(hint -> missingCharacters.addAll(
                hint
                .chars()
                .mapToObj(c -> (char) c)
                .filter(c -> !passwordCrackingJob.getRemainingChars().contains(c))
                .collect(Collectors.toSet())));
        passwordCrackingJob.getRemainingChars().removeAll(missingCharacters);
    }

    private void replaceHintHashes(PasswordCrackingJob job, List<Pair<String, String>> resolvedHashes) {
        for (int iHint = 0; iHint < job.getHints().size(); iHint++) {
            for (Pair<String, String> resolved : resolvedHashes) {
                if (job.getHints().get(iHint).equals(resolved.getLeft())) {
                    job.getHints().set(iHint, resolved.getRight());
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

    private final int CHUNK_SIZE = 1024;

    private Queue<Worker.CompareMessage> createHintCrackingTasks(PasswordCrackingJob passwordCrackingJob) {
        return createTasks(passwordCrackingJob, passwordCrackingJob.getHints(), passwordCrackingJob.getRemainingChars().size());
    }

    private Queue<Worker.CompareMessage> createTasks(PasswordCrackingJob passwordCrackingJob, LinkedList<String> hashes, int permutationLength) {
        String occurringCharacters = passwordCrackingJob.getRemainingCharsAsString();
        LinkedList<String> permutations = new LinkedList<>();
        heapPermutation(occurringCharacters.toCharArray(), permutationLength, permutations);
        passwordCrackingJob.setPermutations(permutations);
        int totalPermutations = permutations.size();
        if (!getHashStore().containsKey(Pair.of(occurringCharacters, permutationLength)))
            getHashStore().put(Pair.of(occurringCharacters, permutationLength), Arrays.asList(new String[totalPermutations]));

        int chunkCount = (int) Math.ceil((double) totalPermutations / getCHUNK_SIZE());
        Queue<Worker.CompareMessage> hintCrackingTasks = new LinkedList<>();
        for (int iChunk = 0; iChunk < chunkCount; iChunk++) {
            int offset = iChunk * getCHUNK_SIZE();
            hintCrackingTasks.add(new Worker.CompareMessage(
                    offset,
                    (iChunk + 1) * getCHUNK_SIZE() < totalPermutations ? getCHUNK_SIZE() : totalPermutations - offset,
                    permutationLength,
                    occurringCharacters,
                    null, // will be updated with the freshest cache in sendNextTaskToWorker, right before sending
                    hashes,
                    null, // will be updated with the freshest cache in sendNextTaskToWorker, right before sending
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
            task.setHashCache(new LinkedList<>(
                    getHashStore()
                            .get(Pair.of(task.getOccurringCharacters(), task.getPermutationSize()))
                            .subList(task.getOffset(), task.getOffset() + task.getLength())));
            task.setPermutations(new LinkedList<>(
                    getPasswordCrackingJobMap()
                            .get(passwordCrackingJobId).getPermutations()
                            .subList(task.getOffset(), task.getOffset() + task.getLength())));
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
