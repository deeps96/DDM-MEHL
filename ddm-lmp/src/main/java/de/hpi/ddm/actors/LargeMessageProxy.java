package de.hpi.ddm.actors;

import akka.Done;
import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import akka.serialization.Serializers;
import akka.stream.ActorMaterializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import lombok.*;
import scala.util.Try;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class MessageOffer implements Serializable{
		private static final long serialVersionUID = 9051860651669008815L;
		private ActorRef sender;
		private ActorRef receiver;
		private int serializerId;
		private int size;
		private SourceRef<byte[]> sourceRef;
		private String manifest;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(MessageOffer.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		Serialization serialization = SerializationExtension.get(getContext().getSystem());
		Serializer serializer = serialization.findSerializerFor(message.getMessage());
		int serializerId = serializer.identifier();
		String manifest = Serializers.manifestFor(serializer, message.getMessage());
		Try<byte[]> serializeJob = serialization.serialize(message.getMessage());
		if (serializeJob.isSuccess()) {
			Source<byte[], NotUsed> source = Source.from(chunkByteArray(serializeJob.get()));
			CompletionStage<SourceRef<byte[]>> completionStage = source.runWith(StreamRefs.sourceRef(), ActorMaterializer.create(this.context().system()));
			completionStage.whenComplete((sourceRef, e) ->
				receiverProxy.tell(new MessageOffer(this.sender(), message.getReceiver(), serializerId, serializeJob.get().length, sourceRef, manifest), this.self()));
		}
	}

	@Getter(AccessLevel.PRIVATE)
	private final int CHUNK_SIZE = 4096;

	private List<byte[]> chunkByteArray(byte[] in) {
		int chunkCount = (int) Math.ceil(in.length * 1.0 / getCHUNK_SIZE());
		List<byte[]> chunks = new ArrayList<>(chunkCount);
		for (int iChunk = 0; iChunk < chunkCount; iChunk++)
			chunks.add(Arrays.copyOfRange(in, iChunk * getCHUNK_SIZE(), Math.min((iChunk + 1) * getCHUNK_SIZE(), in.length)));
		return chunks;
	}

	private void handle(MessageOffer messageOffer) {
		Serialization serialization = SerializationExtension.get(getContext().getSystem());
		ByteArrayOutputStream messageBuffer = new ByteArrayOutputStream(messageOffer.getSize());
		CompletionStage<Done> done = messageOffer.getSourceRef().getSource().runWith(Sink.foreach(messageBuffer::write), ActorMaterializer.create(this.context().system()));
		done.thenRun(() -> {
			byte[] serialized = messageBuffer.toByteArray();
			messageOffer.getReceiver().tell(serialization.deserialize(serialized, messageOffer.getSerializerId(), messageOffer.getManifest()).get(), messageOffer.getSender());
		});

	}
}
