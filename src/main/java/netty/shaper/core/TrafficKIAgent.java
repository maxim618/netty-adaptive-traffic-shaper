package netty.shaper.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Легковесный AI-агент на базе обучения с подкреплением (Q-learning).
 * Управляет стратегиями фрагментации сетевых буферов.
 */
public class TrafficKIAgent {

    // Таблица памяти ИИ: Состояние (Размер пакета) -> Массив оценок действий (Q-values)
    private final Map<Integer, double[]> qTable = new HashMap<>();
    private final Random random = new Random();

    // Возможные действия ИИ (Варианты максимального размера нарезки чанков в байтах)
    private final int[] actions = {5, 15, 30, 64, 128};

    // Параметры обучения ИИ
    private final double learningRate = 0.1;  // Скорость обучения (альфа)
    private final double discountFactor = 0.9; // Важность будущих наград (гамма)
    private final double epsilon = 0.2;        // Вероятность случайного исследования (exploration)

    /**
     * AI выбирает оптимальный размер чанка для текущего размера буфера
     */
    public int chooseChunkSize(int packetSize) {
        // Округляем размер пакета до ближайшего десятка, чтобы уменьшить размер таблицы состояний
        int state = (packetSize / 10) * 10;

        // Если состояние новое, инициализируем массив действий нулями
        qTable.putIfAbsent(state, new double[actions.length]);

        // Стратегия Epsilon-Greedy: иногда выбираем случайное действие для исследования сети
        if (random.nextDouble() < epsilon) {
            return actions[random.nextInt(actions.length)];
        }

        // В остальных случаях выбираем действие с максимальной оценкой (лучший прошлый опыт)
        double[] qValues = qTable.get(state);
        int bestActionIdx = 0;
        double maxQ = qValues[0];

        for (int i = 1; i < qValues.length; i++) {
            if (qValues[i] > maxQ) {
                maxQ = qValues[i];
                bestActionIdx = i;
            }
        }

        return actions[bestActionIdx];
    }

    /**
     * Обновление памяти AI на основе полученного результата (награды или penalty function)
     */
    public void updateKnowledge(int packetSize, int chosenChunkSize, double reward) {
        int state = (packetSize / 10) * 10;
        double[] qValues = qTable.get(state);

        // Находим индекс действия, которое было совершено
        int actionIdx = 0;
        for (int i = 0; i < actions.length; i++) {
            if (actions[i] == chosenChunkSize) {
                actionIdx = i;
                break;
            }
        }

        // Формула Беллмана для Q-learning
        double oldQ = qValues[actionIdx];
        // В нашем MVP следующее состояние считаем базовым (0), так как сессия короткая
        double maxNextQ = 0.0;

        // Пересчитываем ценность действия
        qValues[actionIdx] = oldQ + learningRate * (reward + discountFactor * maxNextQ - oldQ);

        System.out.printf("[ИИ-Мозг] Обновление памяти для пакета %d байт. Действие %d байт получил оценку: %.4f\n",
                state, chosenChunkSize, qValues[actionIdx]);
    }
}
