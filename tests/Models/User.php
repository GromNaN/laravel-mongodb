<?php

declare(strict_types=1);

namespace Jenssegers\Mongodb\Tests\Models;

use DateTimeInterface;
use Illuminate\Auth\Authenticatable;
use Illuminate\Auth\Passwords\CanResetPassword;
use Illuminate\Contracts\Auth\Authenticatable as AuthenticatableContract;
use Illuminate\Contracts\Auth\CanResetPassword as CanResetPasswordContract;
use Illuminate\Database\Eloquent\Casts\Attribute;
use Illuminate\Notifications\Notifiable;
use Illuminate\Support\Str;
use Jenssegers\Mongodb\Eloquent\HybridRelations;
use Jenssegers\Mongodb\Eloquent\Model as Eloquent;

/**
 * Class User.
 *
 * @property string $_id
 * @property string $name
 * @property string $email
 * @property string $title
 * @property int $age
 * @property \Carbon\Carbon $birthday
 * @property \Carbon\Carbon $created_at
 * @property \Carbon\Carbon $updated_at
 * @property string $username
 * @property MemberStatus member_status
 */
class User extends Eloquent implements AuthenticatableContract, CanResetPasswordContract
{
    use Authenticatable;
    use CanResetPassword;
    use HybridRelations;
    use Notifiable;

    protected $connection = 'mongodb';
    protected $casts = [
        'birthday' => 'datetime',
        'entry.date' => 'datetime',
        'member_status' => MemberStatus::class,
    ];
    protected static $unguarded = true;

    public function books()
    {
        return $this->hasMany(Book::class, 'author_id');
    }

    public function mysqlBooks()
    {
        return $this->hasMany(MysqlBook::class, 'author_id');
    }

    public function items()
    {
        return $this->hasMany(Item::class);
    }

    public function role()
    {
        return $this->hasOne(Role::class);
    }

    public function mysqlRole()
    {
        return $this->hasOne(MysqlRole::class);
    }

    public function clients()
    {
        return $this->belongsToMany(Client::class);
    }

    public function groups()
    {
        return $this->belongsToMany(Group::class, 'groups', 'users', 'groups', '_id', '_id', 'groups');
    }

    public function photos()
    {
        return $this->morphMany(Photo::class, 'has_image');
    }

    public function addresses()
    {
        return $this->embedsMany(Address::class);
    }

    public function father()
    {
        return $this->embedsOne(self::class);
    }

    protected function serializeDate(DateTimeInterface $date)
    {
        return $date->format('l jS \of F Y h:i:s A');
    }

    protected function username(): Attribute
    {
        return Attribute::make(
            get: fn ($value) => $value,
            set: fn ($value) => Str::slug($value)
        );
    }
}
