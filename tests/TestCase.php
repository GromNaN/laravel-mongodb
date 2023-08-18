<?php

declare(strict_types=1);

namespace Jenssegers\Mongodb\Tests;

use Illuminate\Auth\Passwords\PasswordResetServiceProvider as BasePasswordResetServiceProviderAlias;
use Illuminate\Foundation\Application;
use Jenssegers\Mongodb\Auth\PasswordResetServiceProvider;
use Jenssegers\Mongodb\MongodbQueueServiceProvider;
use Jenssegers\Mongodb\MongodbServiceProvider;
use Jenssegers\Mongodb\Tests\Models\User;
use Jenssegers\Mongodb\Validation\ValidationServiceProvider;
use MongoDB\BSON\ObjectId;
use Orchestra\Testbench\TestCase as OrchestraTestCase;

class TestCase extends OrchestraTestCase
{
    public static function assertContainsObjectId(ObjectId $expectedObjectId, array $objectIds, string $message = ''): void
    {
        self::assertNotEmpty($objectIds, $message ?: 'Failed asserting that array of object ids is not empty.');

        foreach ($objectIds as $objectId) {
            if ($objectId == $expectedObjectId) {
                // Found successfully
                return;
            }
        }

        self::fail($message ?: sprintf('Failed asserting that ObjectId(%s) was found in [ObjectId(%s)].', $expectedObjectId, implode('), ObjectId(', $objectIds).')'));
    }

    /**
     * Get application providers.
     *
     * @param  Application  $app
     * @return array
     */
    protected function getApplicationProviders($app)
    {
        $providers = parent::getApplicationProviders($app);

        unset($providers[array_search(BasePasswordResetServiceProviderAlias::class, $providers)]);

        return $providers;
    }

    /**
     * Get package providers.
     *
     * @param  Application  $app
     * @return array
     */
    protected function getPackageProviders($app)
    {
        return [
            MongodbServiceProvider::class,
            MongodbQueueServiceProvider::class,
            PasswordResetServiceProvider::class,
            ValidationServiceProvider::class,
        ];
    }

    /**
     * Define environment setup.
     *
     * @param  Application  $app
     * @return void
     */
    protected function getEnvironmentSetUp($app)
    {
        // reset base path to point to our package's src directory
        //$app['path.base'] = __DIR__ . '/../src';

        $config = require 'config/database.php';

        $app['config']->set('app.key', 'ZsZewWyUJ5FsKp9lMwv4tYbNlegQilM7');

        $app['config']->set('database.default', 'mongodb');
        $app['config']->set('database.connections.mysql', $config['connections']['mysql']);
        $app['config']->set('database.connections.mongodb', $config['connections']['mongodb']);
        $app['config']->set('database.connections.mongodb2', $config['connections']['mongodb']);

        $app['config']->set('auth.model', User::class);
        $app['config']->set('auth.providers.users.model', User::class);
        $app['config']->set('cache.driver', 'array');

        $app['config']->set('queue.default', 'database');
        $app['config']->set('queue.connections.database', [
            'driver' => 'mongodb',
            'table' => 'jobs',
            'queue' => 'default',
            'expire' => 60,
        ]);
        $app['config']->set('queue.failed.database', 'mongodb2');
        $app['config']->set('queue.failed.driver', 'mongodb');
    }
}
