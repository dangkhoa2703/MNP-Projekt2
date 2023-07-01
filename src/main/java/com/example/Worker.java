package com.example;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

// Minh Hieu Le, 222117

public class Worker extends AbstractBehavior<Worker.Message> {

    public interface Message {}

    // assign a multiplication job to a worker
    public record MultiplyAssignMsg(int total, ActorRef<Tasks.Message> tasks) implements Message{}
    // assign an increment job to a worker
    public record IncrementAssignMsg(int num, ActorRef<Worker.Message> multiplyWorker) implements Message{}
    // send the increment's result to multiply
    public record ToMultiplyMsg(int num) implements Message{}

    public static Behavior<Message> create(ActorRef<Scheduler.Message> scheduler) {
        return Behaviors.setup(context -> new Worker(context, scheduler));
    }

    private int counter = 0;
    private final ActorRef<Scheduler.Message> scheduler;
    private ActorRef<Tasks.Message> tasks;
    private int result = 1;
    private int total = 0 ;


    private Worker(ActorContext<Message> context, ActorRef<Scheduler.Message> scheduler) {
        super(context);
        this.scheduler = scheduler;
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(MultiplyAssignMsg.class, this::onMultiplyAssignMsg)
                .onMessage(IncrementAssignMsg.class, this::onIncrementAssignMsg)
                .onMessage(ToMultiplyMsg.class, this::onToMultiplyMsg)
                .build();
    }

    // Assign a multiply job for the worker
    private Behavior<Message> onMultiplyAssignMsg(MultiplyAssignMsg msg) {
        this.tasks = msg.tasks;
        this.total = msg.total;
        if( counter == msg.total ) {
            tasks.tell(new Tasks.ResultMsg(result));
            scheduler.tell(new Scheduler.JobSuccessMsg());
            return Behaviors.stopped();
        }else return this;
    }

    // Assign an increment job for the worker
    private Behavior<Message> onIncrementAssignMsg(IncrementAssignMsg msg) {
        result += msg.num;
        msg.multiplyWorker.tell(new ToMultiplyMsg(result));
        scheduler.tell( new Scheduler.JobSuccessMsg());
        getContext().getLog().info("Increasing job success");
        return Behaviors.stopped();
    }

    // MultiplyWorker multiply the result and then send it back to Tasks
    private Behavior<Message> onToMultiplyMsg(ToMultiplyMsg msg) {
        result *= msg.num;
        counter++;
        if( counter == total ){
            tasks.tell( new Tasks.ResultMsg(result));
            scheduler.tell( new Scheduler.JobSuccessMsg());
            getContext().getLog().info("Multiply job success");
            return Behaviors.stopped();
        }else return this;
    }
}
