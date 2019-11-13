package de.hpi.ddm.actors;

import akka.Done;
import akka.NotUsed;
import akka.actor.*;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import akka.serialization.Serializers;
import akka.stream.ActorMaterializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import akka.stream.serialization.StreamRefSerializer;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.*;
import org.apache.commons.lang3.ArrayUtils;
import scala.util.Try;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.Arrays;
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
	public static class MessageOffer implements Serializable {
		private static final long serialVersionUID = 9051860651669008815L;
		private ActorRef sender;
		private ActorRef receiver;
		private int serializerId;
		private int size;
		private byte[] sourceRef;
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
			Source<Byte, NotUsed> source = Source.from(Arrays.asList(ArrayUtils.toObject(serializeJob.get())));
			CompletionStage<SourceRef<Byte>> completionStage = source.runWith(StreamRefs.sourceRef(), ActorMaterializer.create(this.context().system()));
			completionStage.whenComplete((sourceRef, e) -> {
				StreamRefSerializer streamRefSerializer = new StreamRefSerializer((ExtendedActorSystem) this.context().system());
				receiverProxy.tell(new MessageOffer(this.sender(), message.getReceiver(), serializerId, serializeJob.get().length, streamRefSerializer.toBinary(sourceRef), manifest), this.self());
			});
		}
	}

	private void handle(MessageOffer messageOffer) throws NotSerializableException {
		StreamRefSerializer streamRefSerializer = new StreamRefSerializer((ExtendedActorSystem) this.context().system());
		Serialization serialization = SerializationExtension.get(getContext().getSystem());
		ByteArrayOutputStream messageBuffer = new ByteArrayOutputStream(messageOffer.getSize());
		System.out.println(streamRefSerializer.fromBinary(messageOffer.getSourceRef(), SourceRef.class));
//		SourceRef<Byte> sourceRef = (SourceRef<Byte>) streamRefSerializer.fromBinary(messageOffer.getSourceRef());
//		CompletionStage<Done> done = sourceRef.getSource().runWith(Sink.foreach(messageBuffer::write), ActorMaterializer.create(this.context().system()));
//		done.thenRun(() -> {
//			byte[] serialized = messageBuffer.toByteArray();
//			messageOffer.getReceiver().tell(serialization.deserialize(serialized, messageOffer.getSerializerId(), messageOffer.getManifest()).get(), messageOffer.getSender());
//		});
	}
}
