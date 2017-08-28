/* @flow */
import winston from 'winston';
import Promise from 'bluebird';
import uuidv1 from 'uuid/v1';
import MongoSMQ from './index';

winston.level = 'info';
jasmine.DEFAULT_TIMEOUT_INTERVAL = 60000;

async function setup(options) {
  const fixtures = await (new MongoSMQ(options)).init();
  return fixtures;
}

async function teardown(fixtures) {
  return fixtures.deinit();
}

test('Initiate new MongoSMQ instance', async () => {
  const mongoSQMInstance = await (new MongoSMQ()).init();
  expect(mongoSQMInstance.mongo).not.toBeUndefined();
  mongoSQMInstance.deinit();
});


test('Initiate new MongoSMQ should throw error', async () => {
  expect.assertions(1);
  const mongoSQMInstance = await (new MongoSMQ({ port: 27015 }));
  try {
    await mongoSQMInstance.init();
  } catch (e) {
    return expect(e).toMatchObject({name: 'MongoError'});
  }
  mongoSQMInstance.deinit();
});


test('createMessage method should create new Message', async () => {
  const mongoSQMInstance = await setup();
  const objMsg = {
    a: 'b',
    c: 'd',
  };
  const objCreatedMsg = await mongoSQMInstance.createMessage(objMsg);
  expect(objCreatedMsg.message).toMatchObject(objMsg);
  winston.debug('objCreatedMsg %j', objCreatedMsg);
  const objDeletedMsg = await mongoSQMInstance.removeMessageById(objCreatedMsg);
  winston.debug('objDeletedMsg %j', objDeletedMsg);
  await teardown(mongoSQMInstance);
});


test('getMessage method should get some message', async () => {
  const testTime : number = 10;
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
  winston.debug('Object will created: %j', arrMsg);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseCreatedMsg.push(mongoSQMInstance.createMessage(arrMsg[i]));
  }
  arrCreatedMsg = await Promise.all(arrPromiseCreatedMsg);
  winston.debug('arrCreatedMsg %j', arrCreatedMsg);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseReceivedMsg.push(mongoSQMInstance.getMessage());
  }
  arrReceivedMsg = await Promise.all(arrPromiseReceivedMsg);
  winston.debug('arrReceivedMsg %j', arrReceivedMsg);

  for (let i = 0; i < testTime; i += 1) {
    arrCheck.push(arrMsg.find(obj => obj.a === arrReceivedMsg[i].message.a));
  }
  winston.debug('arrCheck %j', arrCheck);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseDeletedMsg.push(mongoSQMInstance.removeMessageById(arrReceivedMsg[i]));
  }
  arrDeletedMsg = await Promise.all(arrPromiseDeletedMsg);
  winston.debug('arrDeletedMsg %j', arrDeletedMsg);

  expect(arrDeletedMsg.length).toBe(testTime);

  await teardown(mongoSQMInstance);
});


test('getMessage should make messages invisible', async () => {
  const testTime : number = 10;
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
  winston.debug('Object will created: %j', arrMsg);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseCreatedMsg.push(mongoSQMInstance.createMessage(arrMsg[i]));
  }
  arrCreatedMsg = await Promise.all(arrPromiseCreatedMsg);
  winston.debug('arrCreatedMsg %j', arrCreatedMsg);

  let mockFn = null;
  const repeatIn = (ms = 30000) => {
    let countDown = ms;
    return new Promise((resolve) => {
      mockFn = jest.fn().mockImplementation(async () => {
        arrPromiseReceivedMsg = [];
        arrReceivedMsg = [];
        for (let i = 0; i < testTime; i += 1) {
          arrPromiseReceivedMsg.push(mongoSQMInstance.getMessage());
        }
        arrReceivedMsg = await Promise.all(arrPromiseReceivedMsg);
        winston.debug('countDown %d, arrReceivedMsg %j', countDown, arrReceivedMsg);
        if (countDown === 0) {
          clearTimeout(timerId);
          resolve();
          return;
        }
        countDown -= 2500;
      });
      const timerId = setInterval(mockFn, 2500);
    });
  };
  await repeatIn();
  expect(mockFn).toHaveBeenCalledTimes(13);
  const checkEqual = (obj, i) => arrReceivedMsg[i] && arrReceivedMsg[i].message
  && obj.a === arrReceivedMsg[i].message.a;
  for (let i = 0; i < testTime; i += 1) {
    arrCheck.push(arrMsg.find(checkEqual));
  }
  winston.debug('arrCheck %j', arrCheck);

  for (let i = 0; i < testTime; i += 1) {
    arrPromiseDeletedMsg.push(mongoSQMInstance.removeMessageById(arrReceivedMsg[i]));
  }
  arrDeletedMsg = await Promise.all(arrPromiseDeletedMsg);
  winston.debug('arrDeletedMsg %j', arrDeletedMsg);

  expect(arrDeletedMsg.length).toBe(testTime);
  
  await teardown(mongoSQMInstance);
});

