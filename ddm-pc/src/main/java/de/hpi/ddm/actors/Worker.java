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

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static de.hpi.ddm.Utils.hash;

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
		LinkedList<Master.CompareResult.Result> result = findPermutationForHash(
				compareMessage.getHashes(),
				compareMessage.getPermutations());

		this.sender()
				.tell(new Master.CompareResult(result, compareMessage.getJobId()), this.self());
	}

	private LinkedList<Master.CompareResult.Result> findPermutationForHash(List<String> targetHashes, List<String> permutations) {
		LinkedList<Master.CompareResult.Result> resolvedHashes = new LinkedList<>();
		for (String permutation : permutations) {
			for (Iterator<String> targetHashIterator = targetHashes.iterator(); targetHashIterator.hasNext();) {
				String targetHash = targetHashIterator.next();
				if (hash(permutation).equals(targetHash)) {
					resolvedHashes.add(new Master.CompareResult.Result(targetHash, permutation));
					targetHashIterator.remove();
				}
			}
			if (targetHashes.size() == 0) break;
		}
		return resolvedHashes;
	}
}