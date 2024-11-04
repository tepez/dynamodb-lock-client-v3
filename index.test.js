"use strict";

//const clone = require("clone");
const countdown = require("./test/countdown.js");
const crypto = require("crypto");
const DynamoDBLockClient = require("./index.js");

const LOCK_TABLE = "my-lock-table-name";
const PARTITION_KEY = "myPartitionKey";
const SORT_KEY = "mySortKey";
const HEARTBEAT_PERIOD_MS = 10;
const LEASE_DURATION_MS = 100;
const OWNER = "me";
const LOCK_ID = "lockID";
const SORT_ID = "sortID";

describe("FailClosed lock acquisition", () =>
{
    // TODO
});

describe("FailClosed lock release", () =>
{
    // TODO
});

describe("FailOpen lock acquisition", () =>
{
    let config, dynamodb;
    beforeEach(() =>
        {
            config =
            {
                lockTable: LOCK_TABLE,
                partitionKey: PARTITION_KEY,
                heartbeatPeriodMs: HEARTBEAT_PERIOD_MS,
                leaseDurationMs: LEASE_DURATION_MS,
                owner: OWNER
            };
            dynamodb =
            {
                deleteItem: async () => {},
                getItem: async () => {},
                putItem: async () => {}
            }
        }
    );
    describe("using partitionKey", () =>
    {
        describe("gets item from DynamoDB table", () =>
        {
            test("if error, invokes callback with error", done =>
                {
                    const finish = countdown(done, 2);
                    const error = new Error("boom");
                    config.dynamodb = Object.assign(
                        dynamodb,
                        {
                            async getItem(params)
                            {
                                expect(params).toEqual(
                                    {
                                        TableName: LOCK_TABLE,
                                        Key:
                                        {
                                            [PARTITION_KEY]: { S: LOCK_ID }
                                        },
                                        ConsistentRead: true
                                    }
                                );
                                finish();
                                throw error;
                            }
                        }
                    );
                    const failOpen = new DynamoDBLockClient.FailOpen(config);
                    failOpen.acquireLock(LOCK_ID, (err, lock) =>
                        {
                            expect(err).toBe(error);
                            expect(lock).toBe(undefined);
                            finish();
                        }
                    );
                }
            );
            describe("no item present", () =>
            {
                beforeEach(() =>
                    {
                        dynamodb = Object.assign(
                            dynamodb,
                            {
                                getItem: async () => {
                                    return {};
                                }
                            }
                        );
                    }
                );
                describe("puts new item in DynamoDB table", () =>
                {
                    test("if non-ConditionalCheckFailedException error, invokes callback with error", done =>
                        {
                            const finish = countdown(done, 2);
                            const error = new Error("boom");
                            config.dynamodb = Object.assign(
                                dynamodb,
                                {
                                    async putItem(params)
                                    {
                                        expect(params).toEqual(
                                            {
                                                TableName: LOCK_TABLE,
                                                Item:
                                                {
                                                    [PARTITION_KEY]: { S: LOCK_ID },
                                                    fencingToken: { N: "1" },
                                                    leaseDurationMs: { N: LEASE_DURATION_MS.toString() },
                                                    owner: { S: OWNER },
                                                    guid: { S: expect.any(String) }
                                                },
                                                ConditionExpression: `attribute_not_exists(#partitionKey)`,
                                                ExpressionAttributeNames:
                                                {
                                                    "#partitionKey": PARTITION_KEY
                                                }
                                            }
                                        );
                                        finish();
                                        throw error;
                                    }
                                }
                            );
                            const failOpen = new DynamoDBLockClient.FailOpen(config);
                            failOpen.acquireLock(
                                {
                                    [PARTITION_KEY]: LOCK_ID
                                },
                                (err, lock) =>
                                {
                                    expect(err).toBe(error);
                                    expect(lock).toBe(undefined);
                                    finish();
                                }
                            );
                        }
                    );
                    describe("trustLocalTime true, includes lockAcquiredTimeUnixMs parameter", () =>
                    {
                        beforeEach(() =>
                            {
                                config.trustLocalTime = true;
                            }
                        );
                        test("if non-ConditionalCheckFailedException error, invokes callback with error", done =>
                            {
                                const finish = countdown(done, 2);
                                const error = new Error("boom");
                                config.dynamodb = Object.assign(
                                    dynamodb,
                                    {
                                        async putItem(params)
                                        {
                                            expect(params).toEqual(
                                                {
                                                    TableName: LOCK_TABLE,
                                                    Item:
                                                    {
                                                        [PARTITION_KEY]: { S: LOCK_ID },
                                                        fencingToken: { N: "1" },
                                                        leaseDurationMs: { N: LEASE_DURATION_MS.toString() },
                                                        owner: { S: OWNER },
                                                        guid: { S: expect.any(String) },
                                                        lockAcquiredTimeUnixMs: { N: expect.any(String) }
                                                    },
                                                    ConditionExpression: `attribute_not_exists(#partitionKey)`,
                                                    ExpressionAttributeNames:
                                                    {
                                                        "#partitionKey": PARTITION_KEY
                                                    }
                                                }
                                            );
                                            finish();
                                            throw error;
                                        }
                                    }
                                );
                                const failOpen = new DynamoDBLockClient.FailOpen(config);
                                failOpen.acquireLock(
                                    {
                                        [PARTITION_KEY]: LOCK_ID
                                    },
                                    (err, lock) =>
                                    {
                                        expect(err).toBe(error);
                                        expect(lock).toBe(undefined);
                                        finish();
                                    }
                                );
                            }
                        );
                    });
                    describe("if ConditionalCheckFailedException error", () =>
                    {
                        const error = new Error("boom");
                        error.code = "ConditionalCheckFailedException";
                        beforeEach(() =>
                            {
                                dynamodb = Object.assign(
                                    dynamodb,
                                    {
                                        putItem: async () => {
                                            throw error;
                                        }
                                    }
                                );
                            }
                        );
                        describe("default retryCount", () =>
                        {
                            test("retries to get item from DynamoDB table, if error, invokes callback with error", done =>
                                {
                                    let callCount = 0;
                                    const finish = countdown(done, 3);
                                    const err = new Error("boom");
                                    config.dynamodb = Object.assign(
                                        dynamodb,
                                        {
                                            async getItem(params)
                                            {
                                                if (++callCount == 1)
                                                {
                                                    finish();
                                                    return {}; // no item
                                                }
                                                expect(params).toEqual(
                                                    {
                                                        TableName: LOCK_TABLE,
                                                        Key:
                                                        {
                                                            [PARTITION_KEY]: { S: LOCK_ID }
                                                        },
                                                        ConsistentRead: true
                                                    }
                                                );
                                                finish();
                                                throw err;
                                            }
                                        }
                                    );
                                    const failOpen = new DynamoDBLockClient.FailOpen(config);
                                    failOpen.acquireLock(LOCK_ID, (e, lock) =>
                                        {
                                            expect(e).toBe(err);
                                            expect(lock).toBe(undefined);
                                            finish();
                                        }
                                    );
                                }
                            );
                        });
                        describe("out of retries", () =>
                        {
                            beforeEach(() =>
                                {
                                    config.retryCount = 0;
                                }
                            );
                            test("invokes callback with FailedToAcquireLock error", done =>
                                {
                                    config.dynamodb = dynamodb
                                    const failOpen = new DynamoDBLockClient.FailOpen(config);
                                    failOpen.acquireLock(
                                        {
                                            [PARTITION_KEY]: LOCK_ID
                                        },
                                        (err, lock) =>
                                        {
                                            expect(err.message).toBe("Failed to acquire lock.");
                                            expect(err.code).toBe("FailedToAcquireLock");
                                            expect(lock).toBe(undefined);
                                            done();
                                        }
                                    );
                                }
                            );
                        });
                    });
                    test("on success, returns configured Lock", done =>
                        {
                            config.dynamodb = Object.assign(
                                dynamodb,
                                {
                                    putItem: async () => {

                                    }
                                }
                            );
                            const failOpen = new DynamoDBLockClient.FailOpen(config);
                            failOpen.acquireLock(LOCK_ID, (error, lock) =>
                                {
                                    expect(error).toBe(undefined);
                                    expect(lock).toEqual(
                                        expect.objectContaining(
                                            {
                                                _config:
                                                {
                                                    dynamodb: config.dynamodb,
                                                    fencingToken: 1,
                                                    guid: expect.any(String),
                                                    heartbeatPeriodMs: HEARTBEAT_PERIOD_MS,
                                                    leaseDurationMs: LEASE_DURATION_MS,
                                                    lockTable: LOCK_TABLE,
                                                    owner: OWNER,
                                                    partitionID: LOCK_ID,
                                                    partitionKey: PARTITION_KEY,
                                                    type: DynamoDBLockClient.FailOpen
                                                },
                                                _guid: expect.any(String),
                                                _released: false,
                                                fencingToken: 1
                                            }
                                        )
                                    );
                                    done();
                                }
                            );
                        }
                    );
                });
            });
            describe("item present", () =>
            {
                const existingItem =
                {
                    [PARTITION_KEY]: { S: LOCK_ID },
                    fencingToken: { N: "42" },
                    leaseDurationMs: { N: LEASE_DURATION_MS.toString() },
                    owner: { S: `not-${OWNER}` },
                    guid: { S: crypto.randomBytes(64).toString("base64") }
                };
                beforeEach(() =>
                    {
                        dynamodb = Object.assign(
                            dynamodb,
                            {
                                getItem: async () => {
                                    return {
                                        Item: existingItem
                                    };
                                }
                            }
                        );
                    }
                );
                describe("puts updated item in DynamoDB table", () =>
                {
                    test("if non-ConditionalCheckFailedException error, invokes callback with error", done =>
                        {
                            const finish = countdown(done, 2);
                            const error = new Error("boom");
                            config.dynamodb = Object.assign(
                                dynamodb,
                                {
                                    async putItem(params)
                                    {
                                        expect(params).toEqual(
                                            {
                                                TableName: LOCK_TABLE,
                                                Item:
                                                {
                                                    [PARTITION_KEY]: { S: LOCK_ID },
                                                    fencingToken: { N: (parseInt(existingItem.fencingToken.N) + 1).toString() },
                                                    leaseDurationMs: { N: LEASE_DURATION_MS.toString() },
                                                    owner: { S: OWNER },
                                                    guid: { S: expect.any(String) }
                                                },
                                                ConditionExpression: `attribute_not_exists(#partitionKey) or (guid = :guid and fencingToken = :fencingToken)`,
                                                ExpressionAttributeNames:
                                                {
                                                    "#partitionKey": PARTITION_KEY
                                                },
                                                ExpressionAttributeValues:
                                                {
                                                    ":fencingToken": existingItem.fencingToken,
                                                    ":guid": existingItem.guid
                                                }
                                            }
                                        );
                                        expect(params.Item.guid).not.toEqual(existingItem.guid.S);
                                        finish();
                                        throw error;
                                    }
                                }
                            );
                            const failOpen = new DynamoDBLockClient.FailOpen(config);
                            failOpen.acquireLock(
                                {
                                    [PARTITION_KEY]: LOCK_ID
                                },
                                (err, lock) =>
                                {
                                    expect(err).toBe(error);
                                    expect(lock).toBe(undefined);
                                    finish();
                                }
                            );
                        }
                    );
                    describe("trustLocalTime true, includes lockAcquiredTimeUnixMs parameter", () =>
                    {
                        beforeEach(() =>
                            {
                                config.trustLocalTime = true;
                            }
                        );
                        test("if non-ConditionalCheckFailedException error, invokes callback with error", done =>
                            {
                                const finish = countdown(done, 2);
                                const error = new Error("boom");
                                config.dynamodb = Object.assign(
                                    dynamodb,
                                    {
                                        async putItem(params)
                                        {
                                            expect(params).toEqual(
                                                {
                                                    TableName: LOCK_TABLE,
                                                    Item:
                                                    {
                                                        [PARTITION_KEY]: { S: LOCK_ID },
                                                        fencingToken: { N: (parseInt(existingItem.fencingToken.N) + 1).toString() },
                                                        leaseDurationMs: { N: LEASE_DURATION_MS.toString() },
                                                        owner: { S: OWNER },
                                                        guid: { S: expect.any(String) },
                                                        lockAcquiredTimeUnixMs: { N: expect.any(String) }
                                                    },
                                                    ConditionExpression: `attribute_not_exists(#partitionKey) or (guid = :guid and fencingToken = :fencingToken)`,
                                                    ExpressionAttributeNames:
                                                    {
                                                        "#partitionKey": PARTITION_KEY
                                                    },
                                                    ExpressionAttributeValues:
                                                    {
                                                        ":fencingToken": existingItem.fencingToken,
                                                        ":guid": existingItem.guid
                                                    }
                                                }
                                            );
                                            finish();
                                            throw error;
                                        }
                                    }
                                );
                                const failOpen = new DynamoDBLockClient.FailOpen(config);
                                failOpen.acquireLock(
                                    {
                                        [PARTITION_KEY]: LOCK_ID
                                    },
                                    (err, lock) =>
                                    {
                                        expect(err).toBe(error);
                                        expect(lock).toBe(undefined);
                                        finish();
                                    }
                                );
                            }
                        );
                    });
                    describe("if ConditionalCheckFailedException error", () =>
                    {
                        const error = new Error("boom");
                        error.code = "ConditionalCheckFailedException";
                        beforeEach(() =>
                            {
                                dynamodb = Object.assign(
                                    dynamodb,
                                    {
                                        putItem: async () => {
                                            throw error;
                                        }
                                    }
                                );
                            }
                        );
                        describe("default retryCount", () =>
                        {
                            test("retries to get item from DynamoDB table, if error, invokes callback with error", done =>
                                {
                                    let callCount = 0;
                                    const finish = countdown(done, 3);
                                    const err = new Error("boom");
                                    config.dynamodb = Object.assign(
                                        dynamodb,
                                        {
                                            async getItem(params)
                                            {
                                                if (++callCount == 1)
                                                {
                                                    finish();
                                                    return {}; // no item
                                                }
                                                expect(params).toEqual(
                                                    {
                                                        TableName: LOCK_TABLE,
                                                        Key:
                                                        {
                                                            [PARTITION_KEY]: { S: LOCK_ID }
                                                        },
                                                        ConsistentRead: true
                                                    }
                                                );
                                                finish();
                                                throw err;
                                            }
                                        }
                                    );
                                    const failOpen = new DynamoDBLockClient.FailOpen(config);
                                    failOpen.acquireLock(LOCK_ID, (e, lock) =>
                                        {
                                            expect(e).toBe(err);
                                            expect(lock).toBe(undefined);
                                            finish();
                                        }
                                    );
                                }
                            );
                        });
                        describe("out of retries", () =>
                        {
                            beforeEach(() =>
                                {
                                    config.retryCount = 0;
                                }
                            );
                            test("invokes callback with FailedToAcquireLock error", done =>
                                {
                                    config.dynamodb = dynamodb
                                    const failOpen = new DynamoDBLockClient.FailOpen(config);
                                    failOpen.acquireLock(
                                        {
                                            [PARTITION_KEY]: LOCK_ID
                                        },
                                        (err, lock) =>
                                        {
                                            expect(err.message).toBe("Failed to acquire lock.");
                                            expect(err.code).toBe("FailedToAcquireLock");
                                            expect(lock).toBe(undefined);
                                            done();
                                        }
                                    );
                                }
                            );
                        });
                    });
                    test("on success, returns configured Lock", done =>
                        {
                            let newGUID;
                            config.dynamodb = Object.assign(
                                dynamodb,
                                {
                                    async putItem(params)
                                    {
                                        newGUID = params.Item.guid.S;
                                    }
                                }
                            );
                            const failOpen = new DynamoDBLockClient.FailOpen(config);
                            failOpen.acquireLock(LOCK_ID, (error, lock) =>
                                {
                                    expect(error).toBe(undefined);
                                    expect(lock).toEqual(
                                        expect.objectContaining(
                                            {
                                                _config:
                                                {
                                                    dynamodb: config.dynamodb,
                                                    fencingToken: parseInt(existingItem.fencingToken.N) + 1,
                                                    guid: newGUID,
                                                    heartbeatPeriodMs: HEARTBEAT_PERIOD_MS,
                                                    leaseDurationMs: LEASE_DURATION_MS,
                                                    lockTable: LOCK_TABLE,
                                                    owner: OWNER,
                                                    partitionID: LOCK_ID,
                                                    partitionKey: PARTITION_KEY,
                                                    type: DynamoDBLockClient.FailOpen
                                                },
                                                _guid: newGUID,
                                                _released: false,
                                                fencingToken: parseInt(existingItem.fencingToken.N) + 1
                                            }
                                        )
                                    );
                                    done();
                                }
                            );
                        }
                    );
                });
            });
        });
    });
    describe("using partitionKey and sortKey", () =>
    {
        beforeEach(() =>
            {
                config.sortKey = SORT_KEY;
            }
        );
        test("invokes callback with error if configured with sortKey but sortKey value is not provided", done =>
            {
                config.dynamodb = dynamodb;
                const failOpen = new DynamoDBLockClient.FailOpen(config);
                failOpen.acquireLock(
                    {
                        [PARTITION_KEY]: LOCK_ID
                    },
                    (err, lock) =>
                    {
                        expect(err).toEqual(new Error("Lock ID is missing required sortKey value"));
                        done();
                    }
                );
            }
        );
        describe("gets item from DynamoDB table", () =>
        {
            test("if error, invokes callback with error", done =>
                {
                    const finish = countdown(done, 2);
                    const error = new Error("boom");
                    config.dynamodb = Object.assign(
                        dynamodb,
                        {
                            async getItem(params)
                            {
                                expect(params).toEqual(
                                    {
                                        TableName: LOCK_TABLE,
                                        Key:
                                        {
                                            [PARTITION_KEY]: { S: LOCK_ID },
                                            [SORT_KEY]: { S: SORT_ID }
                                        },
                                        ConsistentRead: true
                                    }
                                );
                                finish();
                                throw error;
                            }
                        }
                    );
                    const failOpen = new DynamoDBLockClient.FailOpen(config);
                    failOpen.acquireLock(
                        {
                            [PARTITION_KEY]: LOCK_ID,
                            [SORT_KEY]: SORT_ID
                        },
                        (err, lock) =>
                        {
                            expect(err).toBe(error);
                            expect(lock).toBe(undefined);
                            finish();
                        }
                    );
                }
            );
            describe("no item present", () =>
            {
                beforeEach(() =>
                    {
                        dynamodb = Object.assign(
                            dynamodb,
                            {
                                getItem: async () => {
                                    return {};
                                }
                            }
                        );
                    }
                );
                describe("puts new item in DynamoDB table", () =>
                {
                    test("if non-ConditionalCheckFailedException error, invokes callback with error", done =>
                        {
                            const finish = countdown(done, 2);
                            const error = new Error("boom");
                            config.dynamodb = Object.assign(
                                dynamodb,
                                {
                                    async putItem(params)
                                    {
                                        expect(params).toEqual(
                                            {
                                                TableName: LOCK_TABLE,
                                                Item:
                                                {
                                                    [PARTITION_KEY]: { S: LOCK_ID },
                                                    [SORT_KEY]: { S: SORT_ID },
                                                    fencingToken: { N: "1" },
                                                    leaseDurationMs: { N: LEASE_DURATION_MS.toString() },
                                                    owner: { S: OWNER },
                                                    guid: { S: expect.any(String) }
                                                },
                                                ConditionExpression: `(attribute_not_exists(#partitionKey) and attribute_not_exists(#sortKey))`,
                                                ExpressionAttributeNames:
                                                {
                                                    "#partitionKey": PARTITION_KEY,
                                                    "#sortKey": SORT_KEY
                                                }
                                            }
                                        );
                                        finish();
                                        throw error;
                                    }
                                }
                            );
                            const failOpen = new DynamoDBLockClient.FailOpen(config);
                            failOpen.acquireLock(
                                {
                                    [PARTITION_KEY]: LOCK_ID,
                                    [SORT_KEY]: SORT_ID
                                },
                                (err, lock) =>
                                {
                                    expect(err).toBe(error);
                                    expect(lock).toBe(undefined);
                                    finish();
                                }
                            );
                        }
                    );
                    describe("trustLocalTime true, includes lockAcquiredTimeUnixMs parameter", () =>
                    {
                        beforeEach(() =>
                            {
                                config.trustLocalTime = true;
                            }
                        );
                        test("if non-ConditionalCheckFailedException error, invokes callback with error", done =>
                            {
                                const finish = countdown(done, 2);
                                const error = new Error("boom");
                                config.dynamodb = Object.assign(
                                    dynamodb,
                                    {
                                        async putItem(params)
                                        {
                                            expect(params).toEqual(
                                                {
                                                    TableName: LOCK_TABLE,
                                                    Item:
                                                    {
                                                        [PARTITION_KEY]: { S: LOCK_ID },
                                                        [SORT_KEY]: { S: SORT_ID },
                                                        fencingToken: { N: "1" },
                                                        leaseDurationMs: { N: LEASE_DURATION_MS.toString() },
                                                        owner: { S: OWNER },
                                                        guid: { S: expect.any(String) },
                                                        lockAcquiredTimeUnixMs: { N: expect.any(String) }
                                                    },
                                                    ConditionExpression: `(attribute_not_exists(#partitionKey) and attribute_not_exists(#sortKey))`,
                                                    ExpressionAttributeNames:
                                                    {
                                                        "#partitionKey": PARTITION_KEY,
                                                        "#sortKey": SORT_KEY
                                                    }
                                                }
                                            );
                                            finish();
                                            throw error;
                                        }
                                    }
                                );
                                const failOpen = new DynamoDBLockClient.FailOpen(config);
                                failOpen.acquireLock(
                                    {
                                        [PARTITION_KEY]: LOCK_ID,
                                        [SORT_KEY]: SORT_ID
                                    },
                                    (err, lock) =>
                                    {
                                        expect(err).toBe(error);
                                        expect(lock).toBe(undefined);
                                        finish();
                                    }
                                );
                            }
                        );
                    });
                    describe("if ConditionalCheckFailedException error", () =>
                    {
                        const error = new Error("boom");
                        error.code = "ConditionalCheckFailedException";
                        beforeEach(() =>
                            {
                                dynamodb = Object.assign(
                                    dynamodb,
                                    {
                                        putItem: async () => {
                                            throw error;
                                        }
                                    }
                                );
                            }
                        );
                        describe("default retryCount", () =>
                        {
                            test("retries to get item from DynamoDB table, if error, invokes callback with error", done =>
                                {
                                    let callCount = 0;
                                    const finish = countdown(done, 3);
                                    const err = new Error("boom");
                                    config.dynamodb = Object.assign(
                                        dynamodb,
                                        {
                                            async getItem(params)
                                            {
                                                if (++callCount == 1)
                                                {
                                                    finish();
                                                    return {}; // no item
                                                }
                                                expect(params).toEqual(
                                                    {
                                                        TableName: LOCK_TABLE,
                                                        Key:
                                                        {
                                                            [PARTITION_KEY]: { S: LOCK_ID },
                                                            [SORT_KEY]: { S: SORT_ID }
                                                        },
                                                        ConsistentRead: true
                                                    }
                                                );
                                                finish();
                                                throw err;
                                            }
                                        }
                                    );
                                    const failOpen = new DynamoDBLockClient.FailOpen(config);
                                    failOpen.acquireLock(
                                        {
                                            [PARTITION_KEY]: LOCK_ID,
                                            [SORT_KEY]: SORT_ID
                                        },
                                        (e, lock) =>
                                        {
                                            expect(e).toBe(err);
                                            expect(lock).toBe(undefined);
                                            finish();
                                        }
                                    );
                                }
                            );
                        });
                        describe("out of retries", () =>
                        {
                            beforeEach(() =>
                                {
                                    config.retryCount = 0;
                                }
                            );
                            test("invokes callback with FailedToAcquireLock error", done =>
                                {
                                    config.dynamodb = dynamodb
                                    const failOpen = new DynamoDBLockClient.FailOpen(config);
                                    failOpen.acquireLock(
                                        {
                                            [PARTITION_KEY]: LOCK_ID,
                                            [SORT_KEY]: SORT_ID
                                        },
                                        (err, lock) =>
                                        {
                                            expect(err.message).toBe("Failed to acquire lock.");
                                            expect(err.code).toBe("FailedToAcquireLock");
                                            expect(lock).toBe(undefined);
                                            done();
                                        }
                                    );
                                }
                            );
                        });
                    });
                    test("on success, returns configured Lock", done =>
                        {
                            config.dynamodb = Object.assign(
                                dynamodb,
                                {
                                    putItem: async () => {

                                    }
                                }
                            );
                            const failOpen = new DynamoDBLockClient.FailOpen(config);
                            failOpen.acquireLock(
                                {
                                    [PARTITION_KEY]: LOCK_ID,
                                    [SORT_KEY]: SORT_ID
                                },
                                (error, lock) =>
                                {
                                    expect(error).toBe(undefined);
                                    expect(lock).toEqual(
                                        expect.objectContaining(
                                            {
                                                _config:
                                                {
                                                    dynamodb: config.dynamodb,
                                                    fencingToken: 1,
                                                    guid: expect.any(String),
                                                    heartbeatPeriodMs: HEARTBEAT_PERIOD_MS,
                                                    leaseDurationMs: LEASE_DURATION_MS,
                                                    lockTable: LOCK_TABLE,
                                                    owner: OWNER,
                                                    partitionID: LOCK_ID,
                                                    partitionKey: PARTITION_KEY,
                                                    sortID: SORT_ID,
                                                    sortKey: SORT_KEY,
                                                    type: DynamoDBLockClient.FailOpen
                                                },
                                                _guid: expect.any(String),
                                                _released: false,
                                                fencingToken: 1
                                            }
                                        )
                                    );
                                    done();
                                }
                            );
                        }
                    );
                });
            });
            describe("item present", () =>
            {
                const existingItem =
                {
                    [PARTITION_KEY]: { S: LOCK_ID },
                    [SORT_KEY]: { S: SORT_ID },
                    fencingToken: { N: "42" },
                    leaseDurationMs: { N: LEASE_DURATION_MS.toString() },
                    owner: { S: `not-${OWNER}` },
                    guid: { S: crypto.randomBytes(64).toString("base64") }
                };
                beforeEach(() =>
                    {
                        dynamodb = Object.assign(
                            dynamodb,
                            {
                                getItem: async () => {
                                    return {
                                        Item: existingItem
                                    };
                                }
                            }
                        );
                    }
                );
                describe("puts updated item in DynamoDB table", () =>
                {
                    test("if non-ConditionalCheckFailedException error, invokes callback with error", done =>
                        {
                            const finish = countdown(done, 2);
                            const error = new Error("boom");
                            config.dynamodb = Object.assign(
                                dynamodb,
                                {
                                    async putItem(params)
                                    {
                                        expect(params).toEqual(
                                            {
                                                TableName: LOCK_TABLE,
                                                Item:
                                                {
                                                    [PARTITION_KEY]: { S: LOCK_ID },
                                                    [SORT_KEY]: { S: SORT_ID },
                                                    fencingToken: { N: (parseInt(existingItem.fencingToken.N) + 1).toString() },
                                                    leaseDurationMs: { N: LEASE_DURATION_MS.toString() },
                                                    owner: { S: OWNER },
                                                    guid: { S: expect.any(String) }
                                                },
                                                ConditionExpression: `(attribute_not_exists(#partitionKey) and attribute_not_exists(#sortKey)) or (guid = :guid and fencingToken = :fencingToken)`,
                                                ExpressionAttributeNames:
                                                {
                                                    "#partitionKey": PARTITION_KEY,
                                                    "#sortKey": SORT_KEY
                                                },
                                                ExpressionAttributeValues:
                                                {
                                                    ":fencingToken": existingItem.fencingToken,
                                                    ":guid": existingItem.guid
                                                }
                                            }
                                        );
                                        expect(params.Item.guid).not.toEqual(existingItem.guid);
                                        finish();
                                        throw error;
                                    }
                                }
                            );
                            const failOpen = new DynamoDBLockClient.FailOpen(config);
                            failOpen.acquireLock(
                                {
                                    [PARTITION_KEY]: LOCK_ID,
                                    [SORT_KEY]: SORT_ID
                                },
                                (err, lock) =>
                                {
                                    expect(err).toBe(error);
                                    expect(lock).toBe(undefined);
                                    finish();
                                }
                            );
                        }
                    );
                    describe("trustLocalTime true, includes lockAcquiredTimeUnixMs parameter", () =>
                    {
                        beforeEach(() =>
                            {
                                config.trustLocalTime = true;
                            }
                        );
                        test("if non-ConditionalCheckFailedException error, invokes callback with error", done =>
                            {
                                const finish = countdown(done, 2);
                                const error = new Error("boom");
                                config.dynamodb = Object.assign(
                                    dynamodb,
                                    {
                                        async putItem(params)
                                        {
                                            expect(params).toEqual(
                                                {
                                                    TableName: LOCK_TABLE,
                                                    Item:
                                                    {
                                                        [PARTITION_KEY]: { S: LOCK_ID },
                                                        [SORT_KEY]: { S: SORT_ID },
                                                        fencingToken: { N: (parseInt(existingItem.fencingToken.N) + 1).toString() },
                                                        leaseDurationMs: { N: LEASE_DURATION_MS.toString() },
                                                        owner: { S: OWNER },
                                                        guid: { S: expect.any(String) },
                                                        lockAcquiredTimeUnixMs: { N: expect.any(String) }
                                                    },
                                                    ConditionExpression: `(attribute_not_exists(#partitionKey) and attribute_not_exists(#sortKey)) or (guid = :guid and fencingToken = :fencingToken)`,
                                                    ExpressionAttributeNames:
                                                    {
                                                        "#partitionKey": PARTITION_KEY,
                                                        "#sortKey": SORT_KEY
                                                    },
                                                    ExpressionAttributeValues:
                                                    {
                                                        ":fencingToken": existingItem.fencingToken,
                                                        ":guid": existingItem.guid
                                                    }
                                                }
                                            );
                                            finish();
                                            throw error;
                                        }
                                    }
                                );
                                const failOpen = new DynamoDBLockClient.FailOpen(config);
                                failOpen.acquireLock(
                                    {
                                        [PARTITION_KEY]: LOCK_ID,
                                        [SORT_KEY]: SORT_ID
                                    },
                                    (err, lock) =>
                                    {
                                        expect(err).toBe(error);
                                        expect(lock).toBe(undefined);
                                        finish();
                                    }
                                );
                            }
                        );
                    });
                    describe("if ConditionalCheckFailedException error", () =>
                    {
                        const error = new Error("boom");
                        error.code = "ConditionalCheckFailedException";
                        beforeEach(() =>
                            {
                                dynamodb = Object.assign(
                                    dynamodb,
                                    {
                                        putItem: async () => {
                                            throw error;
                                        }
                                    }
                                );
                            }
                        );
                        describe("default retryCount", () =>
                        {
                            test("retries to get item from DynamoDB table, if error, invokes callback with error", done =>
                                {
                                    let callCount = 0;
                                    const finish = countdown(done, 3);
                                    const err = new Error("boom");
                                    config.dynamodb = Object.assign(
                                        dynamodb,
                                        {
                                            async getItem(params)
                                            {
                                                if (++callCount == 1)
                                                {
                                                    finish();
                                                    return {}; // no item
                                                }
                                                expect(params).toEqual(
                                                    {
                                                        TableName: LOCK_TABLE,
                                                        Key:
                                                        {
                                                            [PARTITION_KEY]: { S: LOCK_ID },
                                                            [SORT_KEY]: { S: SORT_ID }
                                                        },
                                                        ConsistentRead: true
                                                    }
                                                );
                                                finish();
                                                throw err;
                                            }
                                        }
                                    );
                                    const failOpen = new DynamoDBLockClient.FailOpen(config);
                                    failOpen.acquireLock(
                                        {
                                            [PARTITION_KEY]: LOCK_ID,
                                            [SORT_KEY]: SORT_ID
                                        },
                                        (e, lock) =>
                                        {
                                            expect(e).toBe(err);
                                            expect(lock).toBe(undefined);
                                            finish();
                                        }
                                    );
                                }
                            );
                        });
                        describe("out of retries", () =>
                        {
                            beforeEach(() =>
                                {
                                    config.retryCount = 0;
                                }
                            );
                            test("invokes callback with FailedToAcquireLock error", done =>
                                {
                                    config.dynamodb = dynamodb
                                    const failOpen = new DynamoDBLockClient.FailOpen(config);
                                    failOpen.acquireLock(
                                        {
                                            [PARTITION_KEY]: LOCK_ID,
                                            [SORT_KEY]: SORT_ID
                                        },
                                        (err, lock) =>
                                        {
                                            expect(err.message).toBe("Failed to acquire lock.");
                                            expect(err.code).toBe("FailedToAcquireLock");
                                            expect(lock).toBe(undefined);
                                            done();
                                        }
                                    );
                                }
                            );
                        });
                    });
                    test("on success, returns configured Lock", done =>
                        {
                            let newGUID;
                            config.dynamodb = Object.assign(
                                dynamodb,
                                {
                                    async putItem(params)
                                    {
                                        newGUID = params.Item.guid.S;
                                    }
                                }
                            );
                            const failOpen = new DynamoDBLockClient.FailOpen(config);
                            failOpen.acquireLock(
                                {
                                    [PARTITION_KEY]: LOCK_ID,
                                    [SORT_KEY]: SORT_ID
                                },
                                (error, lock) =>
                                {
                                    expect(error).toBe(undefined);
                                    expect(lock).toEqual(
                                        expect.objectContaining(
                                            {
                                                _config:
                                                {
                                                    dynamodb: config.dynamodb,
                                                    fencingToken: parseInt(existingItem.fencingToken.N) + 1,
                                                    guid: newGUID,
                                                    heartbeatPeriodMs: HEARTBEAT_PERIOD_MS,
                                                    leaseDurationMs: LEASE_DURATION_MS,
                                                    lockTable: LOCK_TABLE,
                                                    owner: OWNER,
                                                    partitionID: LOCK_ID,
                                                    partitionKey: PARTITION_KEY,
                                                    sortID: SORT_ID,
                                                    sortKey: SORT_KEY,
                                                    type: DynamoDBLockClient.FailOpen
                                                },
                                                _guid: newGUID,
                                                _released: false,
                                                fencingToken: parseInt(existingItem.fencingToken.N) + 1
                                            }
                                        )
                                    );
                                    done();
                                }
                            );
                        }
                    );
                });
            });
        });
    });
});

describe("FailOpen lock release", () =>
{
    let config, dynamodb;
    beforeEach(() =>
        {
            config =
            {
                lockTable: LOCK_TABLE,
                partitionKey: PARTITION_KEY,
                heartbeatPeriodMs: 1e4,
                leaseDurationMs: 1e5,
                owner: OWNER
            };
            dynamodb =
            {
                deleteItem: async () => {},
                getItem: async () => {},
                putItem: async () => {}
            }
        }
    );
    describe("using partitionKey", () =>
    {
        let guid, lock;
        beforeEach(() =>
            {
                guid = crypto.randomBytes(64).toString("base64");
                lock = new DynamoDBLockClient.Lock(
                    {
                        dynamodb,
                        fencingToken: 42,
                        guid,
                        heartbeatPeriodMs: config.heartbeatPeriodMs,
                        leaseDurationMs: config.leaseDurationMs,
                        lockTable: config.lockTable,
                        owner: config.owner,
                        partitionID: LOCK_ID,
                        partitionKey: config.partitionKey,
                        type: DynamoDBLockClient.FailOpen
                    }
                );
            }
        );
        describe("puts updated item with leaseDurationMs set to 1", () =>
        {
            test("if non-ConditionalCheckFailedException error, invokes callback with error", done =>
                {
                    const finish = countdown(done, 2);
                    const error = new Error("boom");
                    config.dynamodb = Object.assign(
                        dynamodb,
                        {
                            async putItem(params)
                            {
                                expect(params).toEqual(
                                    {
                                        TableName: LOCK_TABLE,
                                        Item:
                                        {
                                            [PARTITION_KEY]: { S: LOCK_ID },
                                            fencingToken: { N: "42" },
                                            leaseDurationMs: { N: "1" },
                                            owner: { S: OWNER },
                                            guid: { S: guid }
                                        },
                                        ConditionExpression: `attribute_exists(#partitionKey) and guid = :guid`,
                                        ExpressionAttributeNames:
                                        {
                                            "#partitionKey": PARTITION_KEY
                                        },
                                        ExpressionAttributeValues:
                                        {
                                            ":guid": { S: guid }
                                        }
                                    }
                                );
                                finish();
                                throw error;
                            }
                        }
                    );
                    expect(lock.heartbeatTimeout).not.toBe(undefined);
                    lock.release(err =>
                        {
                            expect(err).toBe(error);
                            expect(lock.heartbeatTimeout).toBe(undefined);
                            finish();
                        }
                    );
                }
            );
        });
    });
    describe("using partitionKey and sortKey", () =>
    {
        let guid, lock;
        beforeEach(() =>
            {
                config.sortKey = SORT_KEY;
                guid = crypto.randomBytes(64).toString("base64");
                lock = new DynamoDBLockClient.Lock(
                    {
                        dynamodb,
                        fencingToken: 42,
                        guid,
                        heartbeatPeriodMs: config.heartbeatPeriodMs,
                        leaseDurationMs: config.leaseDurationMs,
                        lockTable: config.lockTable,
                        owner: config.owner,
                        partitionID: LOCK_ID,
                        partitionKey: config.partitionKey,
                        sortID: SORT_ID,
                        sortKey: config.sortKey,
                        type: DynamoDBLockClient.FailOpen
                    }
                );
            }
        );
        test("if non-ConditionalCheckFailedException error, invokes callback with error", done =>
            {
                const finish = countdown(done, 2);
                const error = new Error("boom");
                config.dynamodb = Object.assign(
                    dynamodb,
                    {
                        async putItem(params)
                        {
                            expect(params).toEqual(
                                {
                                    TableName: LOCK_TABLE,
                                    Item:
                                    {
                                        [PARTITION_KEY]: { S: LOCK_ID },
                                        [SORT_KEY]: { S: SORT_ID },
                                        fencingToken: { N: "42" },
                                        leaseDurationMs: { N: "1" },
                                        owner: { S: OWNER },
                                        guid: { S: guid }
                                    },
                                    ConditionExpression: `(attribute_exists(#partitionKey) and attribute_exists(#sortKey)) and guid = :guid`,
                                    ExpressionAttributeNames:
                                    {
                                        "#partitionKey": PARTITION_KEY,
                                        "#sortKey": SORT_KEY
                                    },
                                    ExpressionAttributeValues:
                                    {
                                        ":guid": { S: guid }
                                    }
                                }
                            );
                            finish();
                            throw error;
                        }
                    }
                );
                expect(lock.heartbeatTimeout).not.toBe(undefined);
                lock.release(err =>
                    {
                        expect(err).toBe(error);
                        expect(lock.heartbeatTimeout).toBe(undefined);
                        finish();
                    }
                );
            }
        );
    });
});
