<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Connectors;

use Illuminate\Support\Arr;
use Interop\Amqp\AmqpConnectionFactory as InteropAmqpConnectionFactory;
use Interop\Amqp\AmqpContext;
use InvalidArgumentException;
use Enqueue\AmqpTools\DelayStrategyAware;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\Events\WorkerStopping;
use Illuminate\Queue\Connectors\ConnectorInterface;
use VladimirYuldashev\LaravelQueueRabbitMQ\AliyunCredentialsProvider;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;
use Enqueue\AmqpLib\AmqpConnectionFactory as EnqueueAmqpConnectionFactory;

class RabbitMQConnector implements ConnectorInterface
{
    /**
     * @var Dispatcher
     */
    private $dispatcher;

    public function __construct(Dispatcher $dispatcher)
    {
        $this->dispatcher = $dispatcher;
    }

    /**
     * Establish a queue connection.
     *
     * @param array $config
     *
     * @return RabbitMQQueue
     * @throws \ReflectionException
     */
    public function connect(array $config): RabbitMQQueue
    {
        $factoryClass = Arr::get($config, 'factory_class', EnqueueAmqpConnectionFactory::class);

        if (!class_exists($factoryClass) || !(new \ReflectionClass($factoryClass))->implementsInterface(InteropAmqpConnectionFactory::class)) {
            throw new \LogicException(sprintf('The factory_class option has to be valid class that implements "%s"', InteropAmqpConnectionFactory::class));
        }

        //For Aliyun
        $aliyunAk = Arr::get($config, 'aliyun_access_key');
        $aliyunAs = Arr::get($config, 'aliyun_access_secret');
        $aliyunOwnerId = Arr::get($config, 'aliyun_resouce_owner_id');
        if ($aliyunAk && $aliyunAs && $aliyunOwnerId) {
            $config['login'] = AliyunCredentialsProvider::getUser($aliyunOwnerId, $aliyunAk);
            $config['password'] = AliyunCredentialsProvider::getPassword($aliyunAs);
        }

        /** @var InteropAmqpConnectionFactory $factory */
        $factory = app()->make(
            $factoryClass,
            [
                'config' => [
                    'dsn' => Arr::get($config, 'dsn'),
                    'host' => Arr::get($config, 'host', '127.0.0.1'),
                    'port' => Arr::get($config, 'port', 5672),
                    'user' => Arr::get($config, 'login', 'guest'),
                    'pass' => Arr::get($config, 'password', 'guest'),
                    'vhost' => Arr::get($config, 'vhost', '/'),
                    'ssl_on' => Arr::get($config, 'ssl_params.ssl_on', false),
                    'ssl_verify' => Arr::get($config, 'ssl_params.verify_peer', true),
                    'ssl_cacert' => Arr::get($config, 'ssl_params.cafile'),
                    'ssl_cert' => Arr::get($config, 'ssl_params.local_cert'),
                    'ssl_key' => Arr::get($config, 'ssl_params.local_key'),
                    'ssl_passphrase' => Arr::get($config, 'ssl_params.passphrase'),
                    'persisted' => Arr::get($config, 'persisted', false),
                ],
            ]
        );
        if (!app()->has($factoryClass)) {
            app()->singleton($factoryClass, function () use ($factory) {
                return $factory;
            });
        }

        if ($factory instanceof DelayStrategyAware) {
            //For Aliyun
            $delayStrategy = Arr::get($config, 'delay_strategy');
            if (isset($delayStrategy)) {
                $factory->setDelayStrategy(new $delayStrategy());
            }
        }

        /** @var AmqpContext $context */
        $context = $factory->createContext();

        if ($this->dispatcher->hasListeners(WorkerStopping::class)) {
            $this->dispatcher->forget(WorkerStopping::class);
        }
        $this->dispatcher->listen(WorkerStopping::class, function () use ($context) {
            //Don't close persistent connection
            if ($context instanceof \Enqueue\AmqpExt\AmqpContext) {
                if ($context->getExtChannel()->getConnection()->isPersistent()) {
                    return;
                }
            }

            $context->close();
        });

        $worker = Arr::get($config, 'worker', 'default');

        if ($worker === 'default') {
            return new RabbitMQQueue($context, $config);
        }

        throw new InvalidArgumentException('Invalid worker.');
    }
}
