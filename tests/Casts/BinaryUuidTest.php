<?php

namespace Jenssegers\Mongodb\Tests\Casts;

use Generator;
use function hex2bin;
use Jenssegers\Mongodb\Tests\Models\CastBinaryUuid;
use Jenssegers\Mongodb\Tests\Models\IdCastBinaryUuid;
use Jenssegers\Mongodb\Tests\TestCase;
use MongoDB\BSON\Binary;

class BinaryUuidTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        CastBinaryUuid::truncate();
        IdCastBinaryUuid::truncate();
    }

    /** @dataProvider provideBinaryUuidCast */
    public function testBinaryUuidCastModel(string $expectedUuid, string|Binary $saveUuid, Binary $queryUuid): void
    {
        CastBinaryUuid::create(['uuid' => $saveUuid]);

        $model = CastBinaryUuid::firstWhere('uuid', $queryUuid);
        $this->assertNotNull($model);
        $this->assertSame($expectedUuid, $model->uuid);
    }

    /** @dataProvider provideBinaryUuidCast */
    public function testBinaryUuidCastId(string $expectedUuid, string|Binary $saveUuid, Binary $queryUuid): void
    {
        IdCastBinaryUuid::create(['_id' => $saveUuid]);

        $model = IdCastBinaryUuid::firstWhere('_id', $queryUuid);
        $this->assertNotNull($model);
        $this->assertSame($expectedUuid, $model->_id);
        $this->assertSame($expectedUuid, $model->id);
    }

    public static function provideBinaryUuidCast(): Generator
    {
        $uuid = '0c103357-3806-48c9-a84b-867dcb625cfb';
        $binaryUuid = new Binary(hex2bin('0c103357380648c9a84b867dcb625cfb'), Binary::TYPE_UUID);

        yield 'Save Binary, Query Binary' => [$uuid, $binaryUuid, $binaryUuid];
        yield 'Save string, Query Binary' => [$uuid, $uuid, $binaryUuid];
    }

    public function testQueryByStringDoesNotCast(): void
    {
        $uuid = '0c103357-3806-48c9-a84b-867dcb625cfb';

        CastBinaryUuid::create(['uuid' => $uuid]);

        $model = CastBinaryUuid::firstWhere('uuid', $uuid);
        $this->assertNull($model);
    }
}
