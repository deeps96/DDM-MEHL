package de.hpi.ddm.actors;

import akka.actor.*;
import de.hpi.ddm.structures.PasswordCrackingJob;
import lombok.*;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.*;
import java.util.stream.IntStream;

@Getter(AccessLevel.PRIVATE)
public class Master extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";

    public static Props props(final ActorRef reader, final ActorRef collector) {
        return Props.create(Master.class, () -> new Master(reader, collector));
    }

    public Master(final ActorRef reader, final ActorRef collector) {
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
    public static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<String[]> lines;
    }

    @Data
    public static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
    }

    @Data @AllArgsConstructor @NoArgsConstructor
    public static class StoreHashesMessage implements Serializable {
        private static final long serialVersionUID = -4715813113760725017L;

        private int offset;
        private String occurringCharacters;
        private List<String> hashes;
    }

    @Data @AllArgsConstructor @NoArgsConstructor
    public static class CompareResult implements Serializable {
        private static final long serialVersionUID = 1294419813760526676L;
        private List<Pair<String, String>> resolvedHashes;
        private UUID jobId;

        public boolean hasResult() {
            return getResolvedHashes().size() > 0;
        }
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef reader;
    private final ActorRef collector;
    private final List<ActorRef> workers;

    private long startTime;

    private final HashMap<String, List<String>> hashStore = new HashMap<>();
    private final HashMap<UUID, PasswordCrackingJob> passwordCrackingJobMap = new HashMap<>();
    private final HashMap<UUID, Queue<Worker.CompareMessage>> tasks = new HashMap<>();
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

    protected void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();
        getPasswordCrackingJobs().clear();

        getReader().tell(new Reader.ReadMessage(), self());
    }

    protected void handle(BatchMessage message) {
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
        }
        getWorkers().forEach(this::sendNextTaskToWorker);
    }

    private void handle(StoreHashesMessage message) {
        List<String> hashes = getHashStore().get(message.getOccurringCharacters());
        for (int iHash = 0; iHash < message.getHashes().size(); iHash++)
            hashes.set(message.getOffset() + iHash, message.getHashes().get(iHash));
    }

    private void handle(CompareResult message) {
        // TODO

//        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    private final int CSV_HASH_COLUMN_INDEX = 4;
    private final int CSV_OCCURRING_CHARS_COLUMN_INDEX = 2;
    private final int CSV_HINT_START_COLUMN_INDEX = 5;

    private PasswordCrackingJob parsePasswordCrackingJob(String[] line) {
        return new PasswordCrackingJob(
                UUID.randomUUID(),
                line[getCSV_OCCURRING_CHARS_COLUMN_INDEX()],
                line[getCSV_HASH_COLUMN_INDEX()],
                Arrays.asList(Arrays.copyOfRange(line, getCSV_HINT_START_COLUMN_INDEX(), line.length))
        );
    }

    private final int CHUNK_SIZE = 1024;

    private Queue<Worker.CompareMessage> createHintCrackingTasks(PasswordCrackingJob passwordCrackingJob) {
        String occurringCharacters = passwordCrackingJob.getRemainingCharsAsString();
        int totalPermutations = factorial(occurringCharacters.length() - 1);
        if (!getHashStore().containsKey(occurringCharacters))
            getHashStore().put(occurringCharacters, Arrays.asList(new String[totalPermutations]));

        int chunkCount = (int) Math.ceil((double) totalPermutations / getCHUNK_SIZE());
        Queue<Worker.CompareMessage> hintCrackingTasks = new LinkedList<>();
        for (int iChunk = 0; iChunk < chunkCount; iChunk++) {
            int offset = iChunk * getCHUNK_SIZE();
            hintCrackingTasks.add(new Worker.CompareMessage(
                    offset,
                    (iChunk + 1) * getCHUNK_SIZE() < totalPermutations ? getCHUNK_SIZE() : totalPermutations - offset,
                    occurringCharacters,
                    null, // will be updated with the freshest cache in sendNextTaskToWorker, right before sending
                    passwordCrackingJob.getHints(),
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

    private void sendNextTaskToWorker(ActorRef worker, UUID passwordCrackingJobId) {
        Worker.CompareMessage task = getTasks().get(passwordCrackingJobId).poll();
        if (task != null) {
            task.setHashCache(getHashStore().get(task.getOccurringCharacters()).subList(task.getOffset(), task.getOffset() + task.getLength()));
            worker.tell(task, self());
        }
    }

    private int factorial(int n) {
        return IntStream
                .rangeClosed(1, n)
                .reduce(1, (int x, int y) -> x * y);
    }

    protected void handle(RegistrationMessage message) {
        context().watch(sender());
        getWorkers().add(sender());
        sendNextTaskToWorker(sender());
    }

    protected void handle(Terminated message) {
        this.context().unwatch(message.getActor());
        this.workers.remove(message.getActor());
    }

    protected void terminate() {
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
