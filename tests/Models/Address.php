<?php

declare(strict_types=1);

namespace Jenssegers\Mongodb\Tests\Models;

use Jenssegers\Mongodb\Eloquent\Casts\ObjectId as ObjectIdCast;
use Jenssegers\Mongodb\Eloquent\Model as Eloquent;
use Jenssegers\Mongodb\Relations\EmbedsMany;
use MongoDB\BSON\ObjectId;

class Address extends Eloquent
{
    protected $connection = 'mongodb';
    protected static $unguarded = true;
    protected $keyType = ObjectId::class;

    protected $casts = [
        '_id' => ObjectIdCast::class,
    ];

    public function addresses(): EmbedsMany
    {
        return $this->embedsMany(self::class);
    }
}
