package netty.shaper.core;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class AdaptiveTrafficHandler extends ChannelInboundHandlerAdapter {

    // Снабжаем прокси-сервер единым ИИ-мозгом. Статическое поле гарантирует,
    // что ИИ накапливает опыт (память Q-таблицы) между всеми подключениями клиентов.
    private static final TrafficKIAgent aiAgent = new TrafficKIAgent();
    private final Random random = new Random();

    private Channel outboundChannel;
    private int packetCounter = 0;

    // Параметры обфускации трафика
    private final boolean isShapingEnabled = true;

    // Переменные для отслеживания параметров текущего пакета, необходимые для обучения ИИ
    private int lastOriginalPacketSize = 0;
    private int lastChosenChunkSize = 0;

    // Метрики сетевого анализа (Этап 3)
    private long lastPacketTimestamp = 0;
    private long totalBytesInSession = 0;

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // Ожидаем подключения и временно не читаем данные из сокета клиента
        ctx.read();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }

        ByteBuf in = (ByteBuf) msg;

        // Расчет ИИ-метрик (АТ/Интервалы и объемы трафика)
        long currentTimestamp = System.currentTimeMillis();
        int packetSize = in.readableBytes();

        // Вычисляем Inter-Arrival Time (IAT) - задержку между пакетами в мс
        long iat = (lastPacketTimestamp == 0) ? 0 : (currentTimestamp - lastPacketTimestamp);
        lastPacketTimestamp = currentTimestamp;
        totalBytesInSession += packetSize;

        System.out.printf("[AI-Metrics] Входной фрейм #%d -> Размер: %d байт | Интервал: %d мс | Всего: %d байт\n",
                packetCounter + 1, packetSize, iat, totalBytesInSession);

        // Если туннель к удаленному сайту уже активен — перенаправляем поток через ИИ-шейпер
        if (outboundChannel != null && outboundChannel.isActive()) {
            shaperWriteAndFlush(in);
            return;
        }

        // Сохраняем размер первого исходного пакета для последующего обучения ИИ
        this.lastOriginalPacketSize = packetSize;

        // Если это самый первый пакет сессии — это запрос на подключение (обычно HTTP CONNECT или TLS Handshake)
        // Для простоты MVP эмулируем базовый HTTP/SOCKS прокси: вычленяем хост (в реальном коде нужен парсер заголовка)
        // Сейчас для теста жестко свяжем прокси с удаленным эмулятором или тестовым HTTPS сайтом.
        // Перенаправляем весь трафик, например, на демонстрационный эхо-сервер или целевой узел.

        // Маршрут по умолчанию для нашего Transparent Proxy теста
        String remoteHost = "echo.websocket.org"; // Замените на ваш тестовый хост/IP для экспериментов
        int remotePort = 443;

        // Останавливаем чтение от клиента, пока создается исходящий канал
        ctx.channel().config().setAutoRead(false);

        Bootstrap b = new Bootstrap();
        b.group(ctx.channel().eventLoop())
                .channel(ctx.channel().getClass())
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext outboundCtx, Object outboundMsg) {
                                // Если удаленный сервер прислал ответ — стратегия успешна! Поощряем ИИ
                                if (lastOriginalPacketSize > 0 && lastChosenChunkSize > 0) {
                                    System.out.println("[AI-Feedback] 👍 Успех! Получен ответ от сервера. " +
                                            "Регистрация положительного вознаграждения в функции потерь.");
                                    aiAgent.updateKnowledge(lastOriginalPacketSize, lastChosenChunkSize, 10.0);
                                    lastOriginalPacketSize = 0;
                                    lastChosenChunkSize = 0;
                                }
                                ctx.writeAndFlush(outboundMsg);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext outboundCtx) {
                                ctx.close();
                            }

                            // === НОВЫЙ БЛОК: ПЕРЕХВАТ ОШИБОК ИСХОДЯЩЕГО КАНАЛА ===
                            @Override
                            public void exceptionCaught(ChannelHandlerContext outboundCtx, Throwable cause) {
                                // Так как именно этот канал падает с Connection reset, ловим ошибку здесь
                                if (lastOriginalPacketSize > 0 && lastChosenChunkSize > 0) {
                                    System.err.printf("\n[AI-Feedback] \uD83C\uDF88 Achtung - Netto Lock/Borrar‼ " +
                                                    "Пакет %d байт, чанк %d байт. " +
                                                    "Регистрация штрафного коэффициента (Penalty) при деструктивном " +
                                                    "завершении сессии - negative Reinforcement AI.\n",
                                            lastOriginalPacketSize, lastChosenChunkSize);

                                    // Передаем штраф в ИИ-агента
                                    aiAgent.updateKnowledge(lastOriginalPacketSize, lastChosenChunkSize, -10.0);

                                    // Обязательно сбрасываем маркеры сессии
                                    lastOriginalPacketSize = 0;
                                    lastChosenChunkSize = 0;
                                }

                                // Закрываем оба канала связи при аварии
                                outboundCtx.close();
                                ctx.close();
                            }
                        });
                    }
                });


        ChannelFuture f = b.connect(remoteHost, remotePort);
        outboundChannel = f.channel();

        f.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                // Туннель готов, включаем чтение обратно и отправляем первый пакет
                ctx.channel().config().setAutoRead(true);
                shaperWriteAndFlush(in);
            } else {
                ctx.close();
                in.release();
            }
        });
    }

    /**
     * Метод адаптивной модификации трафика на основе решений ИИ-агента
     */
    private void shaperWriteAndFlush(ByteBuf buf) {
        packetCounter++;

        // Применяем защиту к начальной фазе сессии (первые 3 критических пакета рукопожатия)
        if (isShapingEnabled && packetCounter <= 3 && buf.readableBytes() > 30) {
            int originalSize = buf.readableBytes();

            // Ключевой момент -  запрашиваем размер фрагмента у нашего ИИ-агента вместо генерации рандома
            int aiTargetChunkSize = aiAgent.chooseChunkSize(originalSize);
            this.lastChosenChunkSize = aiTargetChunkSize;

            System.out.printf("[Shaper-AI] Защита сессии ИИ. Исходный пакет: %d байт. Модель выбрала размер чанка: %d байт\n",
                    originalSize, aiTargetChunkSize);

            while (buf.readableBytes() > 0) {
                // Нарезаем буфер без копирования данных в памяти (Zero-Copy)
                int chunkSize = Math.min(buf.readableBytes(), aiTargetChunkSize);
                ByteBuf chunk = buf.readRetainedSlice(chunkSize);

                // Добавляем микросекундный джиттер для размытия таймингов сетевого профиля
                int jitterMs = random.nextInt(8) + 2; // 2-10 мс

                outboundChannel.eventLoop().schedule(() -> {
                    outboundChannel.writeAndFlush(chunk);
                }, jitterMs, TimeUnit.MILLISECONDS);
            }
            buf.release();
        } else {
            // Весь последующий фоновый или тяжелый трафик пускаем без изменений
            outboundChannel.writeAndFlush(buf);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (outboundChannel != null) {
            outboundChannel.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Базовый логгер для ошибок со стороны клиента (curl)
        System.err.println("[Inbound-Handler] Сетевое исключение клиента: " + cause.getMessage());
        ctx.close();
    }


}