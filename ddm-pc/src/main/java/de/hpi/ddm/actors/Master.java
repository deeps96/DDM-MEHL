package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import scala.Int;

public class Master extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";
    private int jobIdCounter = 0;

    @Data @NoArgsConstructor
    public class PasswordCrackingJob {
        private int id;
        private String hash;
        private int unresolvedHintCount;
        private List<Character> remainingChars;
        private String crackedPassword;
    }

    private Queue<PasswordCrackingJob> passwordCrackingJobs = new LinkedList<>();
    private HashMap<String, List<String>> hashStore = new HashMap<>();

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

    @Data
    public static class StoreHashesMessage implements Serializable {
        private static final long serialVersionUID = -4715813113760725017L;

        private int offset;
        private String occurringCharacters;
        private List<String> hashes;
    }

    @Data @AllArgsConstructor @NoArgsConstructor
    public static class CompareResult implements Serializable {
        private static final long serialVersionUID = 1294419813760526676L;
        private String matchingPermutation;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef reader;
    private final ActorRef collector;
    private final List<ActorRef> workers;

    private long startTime;

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

        jobIdCounter = 0;
        hashStore.clear();
        passwordCrackingJobs.clear();

        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    protected void handle(BatchMessage message) {

        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        // The input file is read in batches for two reasons: /////////////////////////////////////////////////
        // 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
        // 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
        // TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////////////

        if (message.getLines().isEmpty()) {
            this.collector.tell(new Collector.PrintMessage(), this.self());
            this.terminate();
            return;
        }

        for (String[] line : message.getLines()) {
            startPasswordCrackingJob(line);
        }


        this.reader.tell(new Reader.ReadMessage(), this.self());
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

    protected void handle(StoreHashesMessage message) {
        hashStore.put(message.occurringCharacters, message.hashes);
    }

    protected void handle(CompareResult message) {
        // TODO
    }

    protected void handle(RegistrationMessage message) {
        this.context().watch(this.sender());
        this.workers.add(this.sender());
    }

    protected void handle(Terminated message) {
        this.context().unwatch(message.getActor());
        this.workers.remove(message.getActor());
    }

    private void startPasswordCrackingJob(String[] line) {
        PasswordCrackingJob newJob = new PasswordCrackingJob();
        newJob.setId(jobIdCounter);
        jobIdCounter++;

        newJob.setHash(line[4]);

        String occurringCharacters = line[2];
        newJob.setRemainingChars(occurringCharacters.chars()
                .mapToObj(e->(char)e).collect(Collectors.toList()));

        String[] hints = Arrays.copyOfRange(line, 5, line.length);
        newJob.setUnresolvedHintCount(hints.length);

        passwordCrackingJobs.add(newJob);

        long numberOfPermutations = factorial(occurringCharacters.length() - 1);

        for (String hint : hints) {
            distributeHintCracking(hint, occurringCharacters, numberOfPermutations);
        }
    }

    private void distributeHintCracking(String hint, String occurringCharacters, long numberOfPermutations) {
        int chunkSize = (int) Math.ceil(numberOfPermutations / this.workers.size());

        for(int i = 0; i < this.workers.size(); i++) {
            int offset = i * chunkSize;

            List<String> hashes = hashStore.get(occurringCharacters).subList(offset, offset + chunkSize);

            Worker.CompareMessage compareMessage = new Worker.CompareMessage(offset, chunkSize, hint, occurringCharacters, hashes);

            this.workers.get(i).tell(compareMessage, this.sender());
        }
    }

    private long factorial(int n) {
        return LongStream.rangeClosed(1, n)
                .reduce(1, (long x, long y) -> x * y);
    }
}
