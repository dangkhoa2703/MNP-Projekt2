package com.example;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import java.util.ArrayList;

public class Scheduler extends AbstractBehavior<Scheduler.Message> {

    public interface Message {}

    // receive a total number of worker which is needed for a job and the address of the task
    public record WorkerNumMsg(int workerNum, ActorRef<Tasks.Message> tasks) implements Message{}
    // receive a signal which tell that a worker finished its job
    public record JobSuccessMsg() implements Message{}


    public static Behavior<Message> create() {
        return Behaviors.setup(Scheduler::new);
    }

    int totalCurrentWorker = 0;
    private record Task( int numOfWorker, ActorRef<Tasks.Message> tasks) {}
    ArrayList<Task> taskList = new ArrayList<>();

    private Scheduler(ActorContext<Message> context) {
        super(context);
    }

    //create a list of worker for the next task in taskList if there is enough space left
    private void checkAvailability() {
        if( !taskList.isEmpty()) {
            int neededWorker = taskList.get(0).numOfWorker;
            if (20 - totalCurrentWorker >= neededWorker) {
                Task task = taskList.remove(0);
                ArrayList<ActorRef<Worker.Message>> workerList = new ArrayList<>();
                for (int i = 0; i < task.numOfWorker; i++) {
                    workerList.add(getContext().spawnAnonymous(Worker.create(getContext().getSelf())));
                }
                task.tasks.tell(new Tasks.WorkerListMsg(workerList));
                totalCurrentWorker += neededWorker;
                System.out.println("Created " + neededWorker + " new workers, total current workers: " + totalCurrentWorker);
            }
        }
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(WorkerNumMsg.class, this::onWorkerNumMsg)
                .onMessage(JobSuccessMsg.class, this::onJobSuccess)
                .build();
    }

    // when receive the needed number of workers from Tasks, add it to taskList
    public Behavior<Message> onWorkerNumMsg(WorkerNumMsg msg) {
        taskList.add(new Task(msg.workerNum,msg.tasks));
        getContext().getLog().info("Received needed worker num {}",msg.workerNum);
        checkAvailability();
        return this;
    }

    // decrease total of current worker by 1 when receive a signal from workers
    public Behavior<Message> onJobSuccess(JobSuccessMsg msg) {
        totalCurrentWorker -= 1;
        getContext().getLog().info("Job done,total current workers: {}",totalCurrentWorker);
        checkAvailability();
        return this;
    }
}
