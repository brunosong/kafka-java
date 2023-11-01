package com.example.kafka;


public class AddHookTest {

    static class HookThread extends Thread {
        @Override
        public void run() {
            System.out.println("Hook Run");
        }
    }

    public static void main(String[] args) {

        //메인스레드를 가져온다.
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("HOOK RUN");
                try {
                    mainThread.join();
                } catch (Exception e) {
                    e.getMessage();
                }

            }
        });

    }




}
