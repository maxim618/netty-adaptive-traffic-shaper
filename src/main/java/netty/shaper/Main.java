package netty.shaper;

import netty.shaper.core.TrafficShaperServer;

public class Main {
    public static void main(String[] args) {
        // Локальный порт, который будет слушать наш прокси-сервер
        int port = 8080;

        System.out.println("[Main] Инициализация системы Netty Adaptive Traffic Shaper...");

        try {
            // Создаем и запускаем наш сервер
            TrafficShaperServer server = new TrafficShaperServer(port);
            server.start();
        } catch (InterruptedException e) {
            System.err.println("[Main] Критическая ошибка при работе сервера: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}
