<?php

namespace Jenssegers\Mongodb\Eloquent\Casts;

use Illuminate\Contracts\Database\Eloquent\CastsAttributes;
use Jenssegers\Mongodb\Eloquent\Model;
use MongoDB\BSON\ObjectId as BSONObjectId;
use MongoDB\Driver\Exception\InvalidArgumentException;

/**
 * Store the value as an ObjectId in the database. This cast should be used for _id fields.
 * The value read from the database will not be transformed.
 *
 * @extends CastsAttributes<BSONObjectId, BSONObjectId>
 */
class ObjectId implements CastsAttributes
{
    /**
     * Cast the given value.
     * Nothing will be done here, the value should already be an ObjectId in the database.
     *
     * @param  Model  $model
     * @param  string  $key
     * @param  mixed  $value
     * @param  array  $attributes
     * @return mixed
     */
    public function get($model, string $key, $value, array $attributes)
    {
        if ($value instanceof BSONObjectId && $model->getKeyName() === $key && $model->getKeyType() === 'string') {
            return (string) $value;
        }

        return $value;
    }

    /**
     * Prepare the given value for storage.
     * The value will be converted to an ObjectId.
     *
     * @param  Model  $model
     * @param  string  $key
     * @param  mixed  $value
     * @param  array  $attributes
     * @return mixed
     *
     * @throws \RuntimeException when the value is not an ObjectID or a valid ID string.
     */
    public function set($model, string $key, $value, array $attributes)
    {
        if (! $value instanceof BSONObjectId) {
            if (! is_string($value)) {
                throw new \RuntimeException(sprintf('Invalid BSON ObjectID provided for %s[%s]. "string" or %s expected, got "%s". Remove the ObjectId cast if you need to store other types of values.', get_class($model), $key, BSONObjectId::class, get_debug_type($value)));
            }
            try {
                $value = new BSONObjectId($value);
            } catch (InvalidArgumentException $e) {
                throw new \RuntimeException(sprintf('Invalid BSON ObjectID provided for %s[%s]: %s. Remove the ObjectID cast if you need to store string values.', get_class($model), $key, $value), 0, $e);
            }
        }

        return [$key => $value];
    }
}
