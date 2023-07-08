package com.example;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.util.ArrayList;
import java.util.Arrays;

// Minh Hieu Le, 222117
// Lars Klichta, 232078

public class Tasks extends AbstractBehavior<Tasks.Message> {

    public interface Message {}

    // receive a list of workers from scheduler
    public record WorkerListMsg(ArrayList<ActorRef<Worker.Message>> workerList) implements Message{}
    // receive the final result from MultiplyWorker
    public record ResultMsg(int result) implements Message{}

    public static Behavior<Message> create(ActorRef<Scheduler.Message> scheduler,int[] numList) {
        return Behaviors.setup(context -> new Tasks(context, scheduler, numList));
    }

    private final int[] numList;

    private Tasks(ActorContext<Message> context, ActorRef<Scheduler.Message> scheduler, int[] numList) {
        super(context);
        this.numList = numList;
        scheduler.tell( new Scheduler.WorkerNumMsg(numList.length+1,getContext().getSelf()));
        System.out.println(Arrays.toString(numList));
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(WorkerListMsg.class, this::onWorkerListMsg)
                .onMessage(ResultMsg.class, this::onResultMsg)
                .build();
    }

    // assign job for worker when Tasks receive a worker list from scheduler
    private Behavior<Message> onWorkerListMsg(WorkerListMsg msg){
        ArrayList<ActorRef<Worker.Message>> workerList = msg.workerList;
        ActorRef<Worker.Message> multiplyWorker = workerList.remove(0);
        multiplyWorker.tell( new Worker.MultiplyAssignMsg(numList.length,getContext().getSelf()));
        for( int i = 0; i < workerList.size(); i++ )  {
            workerList.get(i).tell( new Worker.IncrementAssignMsg(numList[i],multiplyWorker));
        }
        return this;
    }

    // print final result on screen when receive it from MultiplyWorker
    private Behavior<Message> onResultMsg(ResultMsg msg){
        getContext().getLog().info( "Result of {}: {} ", Arrays.toString(numList), msg.result);
        return this;
    }
}
