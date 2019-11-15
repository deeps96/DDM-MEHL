package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static de.hpi.ddm.Utils.hash;
import static de.hpi.ddm.Utils.heapPermutation;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor @NoArgsConstructor
	static class CompareMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
		private int offset;
		private int length;
		private int permutationSize;
		private String occurringCharacters;
		private LinkedList<String> hashCache;
		private LinkedList<String> hashes;
		private LinkedList<String> permutations;
		private String jobId;
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);

		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(CompareMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;

			this.getContext()
					.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
					.tell(new Master.RegistrationMessage(), this.self());
		}
	}

	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	private void handle(CompareMessage compareMessage) {
		Pair<Boolean, LinkedList<Pair<String, String>>> result = findPermutationForHash(
				compareMessage.getHashCache(),
				compareMessage.getHashes(),
				compareMessage.getPermutations());

		if (result.getLeft())
			this.getContext()
					.actorSelection(this.masterSystem.address() + "/user/" + Master.DEFAULT_NAME)
					.tell(new Master.StoreHashesMessage(compareMessage.getOffset(), compareMessage.getPermutationSize(), compareMessage.getOccurringCharacters(), compareMessage.getHashCache()), this.self());
		this.sender()
				.tell(new Master.CompareResult(result.getRight(), compareMessage.getJobId()), this.self());
	}

	private Pair<Boolean, LinkedList<Pair<String, String>>> findPermutationForHash(List<String> hashCache, List<String> targetHashes, List<String> permutations) {
		boolean updatedCache = false;
		LinkedList<Pair<String, String>> resolvedHashes = new LinkedList<>();
		for (int iEntry = 0; iEntry < hashCache.size(); iEntry++) {
			if (hashCache.get(iEntry) == null) {
				hashCache.set(iEntry, hash(permutations.get(iEntry)));
				updatedCache = true;
			}
			for (Iterator<String> targetHashIterator = targetHashes.iterator(); targetHashIterator.hasNext();) {
				String targetHash = targetHashIterator.next();
				if (hashCache.get(iEntry).equals(targetHash)) {
					resolvedHashes.add(Pair.of(targetHash, permutations.get(iEntry)));
					targetHashIterator.remove();
				}
			}
			if (targetHashes.size() == 0) break;
		}
		return Pair.of(updatedCache, resolvedHashes);
	}
}