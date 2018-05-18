import * as BPromise from "bluebird";
import * as uuid from "uuid";
import * as winston from "winston";
import MongoSMQ from "./index";

(winston.level as any) = "debug";
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;
const uuidv1 = uuid.v1;

export const repeatIn = (ms: number, interval: number, cb: (() => void)) => {
  let countDown = ms;
  return new Promise((resolve) => {
    const timerId = setInterval(async () => {
      if (countDown === 0) {
        clearTimeout(timerId);
        resolve();
        return;
      }
      await cb();
      countDown -= interval;
    }, interval);
  });
};

/**
 * Returns a random integer between min (inclusive) and max (inclusive)
 * Using Math.round() will give you a non-uniform distribution!
 */
function getRandomInt(min, max) {
  return Math.floor(Math.random() * ((max - min) + 1)) + min;
}

async function setup(options?: any) {
  const opts = Object.assign({}, {
    client: null,
    colName: "Messages",
    db: "abc",
    host: "localhost",
    ns: "rsmq",
    options: {},
    port: 27017,
    visibility: 30,
  }, options);
  const fixtures = await (new MongoSMQ(opts)).init();
  return fixtures;
}

async function teardown(fixtures: MongoSMQ) {
  return fixtures.deinit();
}

test("Initiate new MongoSMQ instance", async () => {
  const mongoSQMInstance = await (new MongoSMQ()).init();
  expect(mongoSQMInstance.mongo).not.toBeUndefined();
  mongoSQMInstance.deinit();
});

test("Initiate new MongoSMQ should throw error", async (done) => {
  try {
    await (new MongoSMQ({ port: 27015 })).init();
  } catch (e) {
    expect(e.name).toBe("MongoNetworkError");
    done();
  }
});

test("createMessage() method should create new Message", async () => {
  const mongoSQMInstance = await setup();
  const objMsg = {
    a: "b",
    c: "d",
  };
  const deleteQuery = {} as any;
  const objCreatedMsg = await mongoSQMInstance.createMessage(objMsg);
  if (objCreatedMsg) {
    if (objCreatedMsg.message) {
      expect(objCreatedMsg.message).toMatchObject(objMsg);
    }
    if (objCreatedMsg._id && typeof objCreatedMsg._id === "object") {
      deleteQuery._id = objCreatedMsg._id.toString();
    }
    if (objCreatedMsg.tries && typeof objCreatedMsg.tries === "number") {
      deleteQuery.tries = objCreatedMsg.tries;
    }
    await mongoSQMInstance.removeMessageById(deleteQuery);
    await teardown(mongoSQMInstance);
  }
});

test("getMessage() method should get some message", async () => {
  const testTime: number = 10;
  const mongoSQMInstance = await setup();
  const arrMsg = [];
  const arrPromiseCreatedMsg = [];
  const arrPromiseReceivedMsg = [];
  const arrPromiseDeletedMsg = [];
  let arrCreatedMsg = [];
  let arrReceivedMsg = [];
  let arrDeletedMsg = [];
  const arrCheck = [];

  for (let i = 0; i < testTime; i += 1) {
    arrMsg.push({
      a: uuidv1(),
    });
  }
  winston.debug("Object will created: %j", arrMsg);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseCreatedMsg.push(mongoSQMInstance.createMessage(arrMsg[i]));
  }
  arrCreatedMsg = await BPromise.all(arrPromiseCreatedMsg);
  winston.debug("arrCreatedMsg %j", arrCreatedMsg);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseReceivedMsg.push(mongoSQMInstance.getMessage());
  }
  arrReceivedMsg = await BPromise.all(arrPromiseReceivedMsg);
  winston.debug("arrReceivedMsg %j", arrReceivedMsg);

  for (let i = 0; i < testTime; i += 1) {
    arrCheck.push(arrMsg.find((obj) => obj.a === arrReceivedMsg[i].message.a));
  }
  winston.debug("arrCheck %j", arrCheck);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseDeletedMsg.push(mongoSQMInstance.removeMessageById(
      {
        _id: arrReceivedMsg[i]._id.toString(),
        tries: arrReceivedMsg[i].tries,
      },
    ));
  }
  arrDeletedMsg = await BPromise.all(arrPromiseDeletedMsg);
  winston.debug("arrDeletedMsg %j", arrDeletedMsg);

  expect(arrDeletedMsg.length).toBe(testTime);
  await teardown(mongoSQMInstance);
});

test("getMessage() should make messages invisible", async () => {
  const testTime: number = 10;
  const mongoSQMInstance = await setup({
    visibility: 10,
  });
  const arrMsg = [];
  const arrPromiseCreatedMsg = [];
  let arrPromiseReceivedMsg = [];
  const arrPromiseDeletedMsg = [];
  let arrCreatedMsg = [];
  let arrReceivedMsg = [];
  let arrDeletedMsg = [];
  const arrCheck = [];

  for (let i = 0; i < testTime; i += 1) {
    arrMsg.push({
      a: uuidv1(),
    });
  }
  winston.debug("Object will created: %j", arrMsg);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseCreatedMsg.push(mongoSQMInstance.createMessage(arrMsg[i]));
  }
  arrCreatedMsg = await BPromise.all(arrPromiseCreatedMsg);
  winston.debug("arrCreatedMsg %j", arrCreatedMsg);

  const mockFn = jest.fn().mockImplementation(async () => {
    arrPromiseReceivedMsg = [];
    arrReceivedMsg = [];
    for (let i = 0; i < testTime; i += 1) {
      arrPromiseReceivedMsg.push(mongoSQMInstance.getMessage());
    }
    arrReceivedMsg = await BPromise.all(arrPromiseReceivedMsg);
    winston.debug(" arrReceivedMsg %j", arrReceivedMsg);
  });
  await repeatIn(32500, 2500, mockFn);
  expect(mockFn).toHaveBeenCalledTimes(13);
  const checkEqual = (obj, i) => arrReceivedMsg[i] && arrReceivedMsg[i].message &&
  obj.a === arrReceivedMsg[i].message.a;
  for (let i = 0; i < testTime; i += 1) {
    arrCheck.push(arrMsg.find(checkEqual));
  }
  winston.debug("arrCheck %j", arrCheck);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseDeletedMsg.push(mongoSQMInstance.removeMessageById(arrReceivedMsg[i]));
  }
  arrDeletedMsg = await BPromise.all(arrPromiseDeletedMsg);
  winston.debug("arrDeletedMsg %j", arrDeletedMsg);

  expect(arrDeletedMsg.length).toBe(testTime);
  await teardown(mongoSQMInstance);
});

test("total() should return correct number of message", async () => {
  const testTime: number = 10;
  const mongoSQMInstance = await setup();
  const arrMsg = [];
  const arrPromiseCreatedMsg = [];
  const arrPromiseReceivedMsg = [];
  const arrPromiseDeletedMsg = [];
  let arrCreatedMsg = [];
  let arrReceivedMsg = [];
  let arrDeletedMsg = [];
  const arrCheck = [];

  for (let i = 0; i < testTime; i += 1) {
    arrMsg.push({
      a: uuidv1(),
    });
  }
  winston.debug("Object will created: %j", arrMsg);

  let numMessage = await mongoSQMInstance.total();
  winston.debug("Number of Initiate Messages %j", numMessage);
  expect(numMessage).toBe(0);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseCreatedMsg.push(mongoSQMInstance.createMessage(arrMsg[i]));
  }
  arrCreatedMsg = await BPromise.all(arrPromiseCreatedMsg);
  winston.debug("arrCreatedMsg %j", arrCreatedMsg);

  numMessage = await mongoSQMInstance.total();
  expect(numMessage).toBe(testTime);
  winston.debug("Number of created Messages %j", numMessage);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseReceivedMsg.push(mongoSQMInstance.getMessage());
  }
  arrReceivedMsg = await BPromise.all(arrPromiseReceivedMsg);
  winston.debug("arrReceivedMsg %j", arrReceivedMsg);
  numMessage = await mongoSQMInstance.total();
  expect(numMessage).toBe(testTime);
  winston.debug("Number of received Messages %j", numMessage);

  for (let i = 0; i < testTime; i += 1) {
    arrCheck.push(arrMsg.find((obj) => obj.a === arrReceivedMsg[i].message.a));
  }
  winston.debug("arrCheck %j", arrCheck);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseDeletedMsg.push(mongoSQMInstance.removeMessageById({
      _id: arrReceivedMsg[i]._id.toString(),
      tries: arrReceivedMsg[i].tries,
    }));
  }
  arrDeletedMsg = await BPromise.all(arrPromiseDeletedMsg);
  winston.debug("arrDeletedMsg %j", arrDeletedMsg);
  numMessage = await mongoSQMInstance.total();
  expect(numMessage).toBe(0);
  winston.debug("Number of messages after deleted %j", numMessage);

  expect(arrDeletedMsg.length).toBe(testTime);

  await teardown(mongoSQMInstance);
});

test("clean() should empty the db", async () => {
  const testTime: number = 10;
  const mongoSQMInstance = await setup();
  const arrMsg = [];
  const arrPromiseCreatedMsg = [];
  let arrCreatedMsg = [];

  for (let i = 0; i < testTime; i += 1) {
    arrMsg.push({
      a: uuidv1(),
    });
  }
  winston.debug("Object will created: %j", arrMsg);

  let numMessage = await mongoSQMInstance.total();
  winston.debug("Number of Initiate Messages %j", numMessage);
  expect(numMessage).toBe(0);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseCreatedMsg.push(mongoSQMInstance.createMessage(arrMsg[i]));
  }
  arrCreatedMsg = await BPromise.all(arrPromiseCreatedMsg);
  winston.debug("arrCreatedMsg %j", arrCreatedMsg);

  numMessage = await mongoSQMInstance.total();
  expect(numMessage).toBe(testTime);
  winston.debug("Number of created Messages %j", numMessage);

  await mongoSQMInstance.clean();
  numMessage = await mongoSQMInstance.total();
  expect(numMessage).toBe(0);
  winston.debug("Number of messages after clean %j", numMessage);

  await teardown(mongoSQMInstance);
});

test("size() should return current available messages", async () => {
  const testTime: number = 20;
  const mongoSQMInstance = await setup();
  const arrMsg = [];
  const arrPromiseCreatedMsg = [];
  const arrPromiseReceivedMsg = [];
  let arrCreatedMsg = [];
  let arrReceivedMsg = [];

  for (let i = 0; i < testTime; i += 1) {
    arrMsg.push({
      a: uuidv1(),
    });
  }
  winston.debug("Object will created: %j", arrMsg);

  let numMessage = await mongoSQMInstance.total();
  winston.debug("Number of Initiate Messages %j", numMessage);
  expect(numMessage).toBe(0);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseCreatedMsg.push(mongoSQMInstance.createMessage(arrMsg[i]));
  }
  arrCreatedMsg = await BPromise.all(arrPromiseCreatedMsg);
  winston.debug("arrCreatedMsg %j", arrCreatedMsg);

  const randNum = getRandomInt(1, 20);
  for (let i = 0; i < randNum; i += 1) {
    arrPromiseReceivedMsg.push(mongoSQMInstance.getMessage());
  }
  arrReceivedMsg = await BPromise.all(arrPromiseReceivedMsg);
  winston.debug("arrReceivedMsg %j", arrReceivedMsg.length);

  const numSize = await mongoSQMInstance.size();
  const numinFlight = await mongoSQMInstance.inFlight();

  expect(numSize).toBe(testTime - randNum);
  winston.debug("Number of size messages %j", numSize);

  expect(numinFlight).toBe(randNum);
  winston.debug("Number of inFlight messages %j", numinFlight);

  expect((numSize as number) + (numinFlight as number)).toBe(testTime);

  await mongoSQMInstance.clean();
  numMessage = await mongoSQMInstance.total();
  expect(numMessage).toBe(0);
  winston.debug("Number of messages after clean %j", numMessage);

  await teardown(mongoSQMInstance);
});

test("updateMessage() should update the message correctly", async () => {
  const testTime: number = 20;
  const mongoSQMInstance = await setup();
  const arrMsg = [];
  const arrPromiseCreatedMsg = [];
  let arrPromiseUpdatedMsg = [];
  let arrPromiseReceivedMsg = [];
  let arrCreatedMsg = [];
  let arrUpdatedMsg = [];
  let arrReceivedMsg = [];

  for (let i = 0; i < testTime; i += 1) {
    arrMsg.push({
      a: uuidv1(),
    });
  }
  winston.debug("Object will created: %j", arrMsg);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseCreatedMsg.push(mongoSQMInstance.createMessage(arrMsg[i]));
  }
  arrCreatedMsg = await BPromise.all(arrPromiseCreatedMsg);
  winston.debug("arrCreatedMsg %j", arrCreatedMsg);

  for (let numTest = 0; numTest < 3; numTest += 1) {
    arrPromiseReceivedMsg = [];
    arrPromiseUpdatedMsg = [];
    for (let i = 0; i < testTime; i += 1) {
      arrPromiseReceivedMsg.push(mongoSQMInstance.getMessage({}, { visibility: 5 }));
    }
    arrReceivedMsg = await BPromise.all(arrPromiseReceivedMsg);
    winston.debug("arrReceivedMsg %j", arrReceivedMsg);

    for (let j = 0; j < testTime; j += 1) {
      arrReceivedMsg[j].message.result = j;
      arrPromiseUpdatedMsg.push(mongoSQMInstance.updateMessage({
        _id: arrReceivedMsg[j]._id.toString(),
        tries: arrReceivedMsg[j].tries,
      }, {
        $set: {
          "message.result": arrReceivedMsg[j].message.result,
        },
      }));
    }
    arrUpdatedMsg = await BPromise.all(arrPromiseUpdatedMsg);
    winston.debug("arrUpdatedMsg %j", arrUpdatedMsg);

    /* sleep some time for message available again */

    await repeatIn(5000, 1000, () => { /*do no thing*/ });
  }

  for (let i = 0; i < testTime; i += 1) {
    expect(arrUpdatedMsg[i].message.result).toBe(i);
    expect(arrUpdatedMsg[i].tries).toBe(3);
  }

  await mongoSQMInstance.clean();
  await teardown(mongoSQMInstance);
});
