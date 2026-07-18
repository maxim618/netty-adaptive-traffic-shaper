package netty.shaper.core;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class TrafficShaperServer {

    private final int port;

    public TrafficShaperServer(int port) {
        this.port = port;
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();

                            // Наш главный адаптивный обработчик, который будет
                            // управлять соединениями и модифицировать байты
                            pipeline.addLast(new AdaptiveTrafficHandler());
                        }
                    })
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();
            System.out.printf("[Shaper-Server] Успешно запущен на порту %d. Ожидание подключений...\n", port);

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
