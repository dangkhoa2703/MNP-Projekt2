package com.example;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;

import java.util.Random;

// Minh Hieu Le, 222117

public class AkkaMainSystem extends AbstractBehavior<AkkaMainSystem.Create> {

    public static class Create {}

    public static Behavior<Create> create() {
        return Behaviors.setup(AkkaMainSystem::new);
    }

    private AkkaMainSystem(ActorContext<Create> context) {
        super(context);
    }

    //create a random array of number
    private int[] createArray() {
        Random random = new Random();
        int r1 = random.nextInt(4,11);
        return random.ints(r1,1,7).toArray();
    }

    @Override
    public Receive<Create> createReceive() {
        return newReceiveBuilder().onMessage(Create.class, this::onCreate).build();
    }

    private Behavior<Create> onCreate(Create command) {
        ActorRef<Scheduler.Message> scheduler = getContext().spawn(Scheduler.create(),"Scheduler");
        for( int i = 0; i < 20; i++) {
            getContext().spawnAnonymous(Tasks.create(scheduler, createArray()));
        }
        return this;
    }
}
