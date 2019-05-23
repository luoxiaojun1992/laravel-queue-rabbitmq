<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Traits;

use Illuminate\Contracts\Bus\Dispatcher;
use Illuminate\Queue\InteractsWithQueue;

trait JobHandler
{
    /**
     * Set the job instance of the given class if necessary.
     *
     * @param  mixed  $instance
     * @return mixed
     */
    protected function setJobInstanceIfNecessary($instance)
    {
        if (in_array(InteractsWithQueue::class, class_uses_recursive($instance))) {
            $instance->setJob($this);
        }

        return $instance;
    }

    /**
     * Ensure the next job in the chain is dispatched if applicable.
     *
     * @param  mixed  $command
     * @return void
     */
    protected function ensureNextJobInChainIsDispatched($command)
    {
        if (method_exists($command, 'dispatchNextJobInChain')) {
            $command->dispatchNextJobInChain();
        }
    }

    /**
     * Handle a model not found exception.
     *
     * @param  \Exception  $e
     * @return void
     */
    protected function handleModelNotFound($e)
    {
        $class = $this->resolveName();

        try {
            $shouldDelete = (new \ReflectionClass($class))
                    ->getDefaultProperties()['deleteWhenMissingModels'] ?? false;
        } catch (\Exception $e) {
            $shouldDelete = false;
        }

        if ($shouldDelete) {
            $this->delete();
        }

        if (class_exists('Illuminate\Queue\FailingJob')) {
            \Illuminate\Queue\FailingJob::handle(
                $this->getConnectionName(), $this, $e
            );
        } else {
            $this->fail($e);
        }
    }

    /**
     * Resolve the handler for the given command.
     *
     * @param  mixed  $command
     * @return mixed
     */
    protected function resolveHandler($command)
    {
        /** @var Dispatcher $dispatcher */
        $dispatcher = $this->resolve(Dispatcher::class);

        $handler = $dispatcher->getCommandHandler($command) ?: null;

        if ($handler) {
            $this->setJobInstanceIfNecessary($handler);
        }

        return $handler;
    }
}
