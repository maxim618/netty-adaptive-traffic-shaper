package netty.shaper.core;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class AdaptiveTrafficHandler extends ChannelInboundHandlerAdapter {

    private Channel outboundChannel;
    private final Random random = new Random();
    private int packetCounter = 0;

    // Параметры обфускации трафика (в будущем ими будет управлять ИИ)
    private final boolean isShapingEnabled = true;

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // Клиент подключился к прокси. Нам нужно временно приостановить чтение,
        // пока мы не откроем туннель к удаленному целевому серверу.
        ctx.read();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }

        ByteBuf in = (ByteBuf) msg;

        // Если туннель к удаленному серверу уже открыт — шлем данные туда
        if (outboundChannel != null && outboundChannel.isActive()) {
            shaperWriteAndFlush(in);
            return;
        }

        // Если это самый первый пакет сессии — это запрос на подключение (обычно HTTP CONNECT или TLS Handshake)
        // Для простоты MVP эмулируем базовый HTTP/SOCKS прокси: вычленяем хост (в реальном коде нужен парсер заголовка)
        // Сейчас для теста жестко свяжем прокси с удаленным эмулятором или тестовым HTTPS сайтом.
        // Перенаправляем весь трафик, например, на демонстрационный эхо-сервер или целевой узел.

        // ВАЖНО: Для полноценного прокси тут парсится хост и порт из первого пакета.
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
                        // Обработчик ответов от удаленного сервера обратно к клиенту
                        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext outboundCtx, Object outboundMsg) {
                                // Все, что вернул реальный сайт, без изменений отдаем обратно браузеру
                                ctx.writeAndFlush(outboundMsg);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext outboundCtx) {
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
     * Метод адаптивного шейпинга/обфускации исходящего потока байт
     */
    private void shaperWriteAndFlush(ByteBuf buf) {
        packetCounter++;

        // ИИ-логика: Цензоры анализируют первые 3-4 пакета (Handshake).
        // Модифицируем только их, чтобы не терять скорость на больших потоках данных.
        if (isShapingEnabled && packetCounter <= 3 && buf.readableBytes() > 30) {
            System.out.printf("[Shaper-AI] Защита сессии! Модификация пакета #%d (%d байт)\n",
                    packetCounter, buf.readableBytes());

            while (buf.readableBytes() > 0) {
                // ИИ-эмуляция: режем пакет на случайные микро-куски от 5 до 25 байт
                int chunkSize = Math.min(buf.readableBytes(), random.nextInt(20) + 5);
                // Метод readRetainedSlice() одновременно с созданием куска увеличивает счетчик ссылок на этот кусок памяти
                ByteBuf chunk = buf.readRetainedSlice(chunkSize);

                // Добавляем случайный тайминговый джиттер от 3 до 12 миллисекунд
                int jitterMs = random.nextInt(9) + 3;

                outboundChannel.eventLoop().schedule(new Runnable() {
                    @Override
                    public void run() {
                        outboundChannel.writeAndFlush(chunk);
                        System.out.println("[Shaper-AI] Отправлен фрагмент: " + chunkSize + " байт");
                    }
                }, jitterMs, TimeUnit.MILLISECONDS);
            }
            buf.release();
        } else {
            // Обычный потоковый трафик пропускаем на полной скорости
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
        System.err.println("[Shaper-Handler] Исключение: " + cause.getMessage());
        ctx.close();
    }
}
