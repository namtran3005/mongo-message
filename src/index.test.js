/* @flow */
import tap from 'tap';
import winston from 'winston';
import Promise from 'bluebird';
import uuidv1 from 'uuid/v1';
import MongoSMQ from './index';

winston.level = 'info';
async function setup(options) {
  const fixtures = await (new MongoSMQ(options)).init();
  return fixtures;
}

async function teardown(fixtures) {
  return fixtures.deinit();
}

tap.test('Initiate new MongoSMQ instance', async (t1) => {
  const mongoSQMInstance = await (new MongoSMQ()).init();
  t1.notEqual(mongoSQMInstance.mongo, undefined, 'After init the connection should be set');
  mongoSQMInstance.deinit();
});

tap.test('Initiate new MongoSMQ should throw error', async (t) => {
  const mongoSQMInstance = await (new MongoSMQ({ port: 27015 }));
  try {
    await mongoSQMInstance.init();
  } catch (e) {
    return t.pass('it throw an error');
  }
  mongoSQMInstance.deinit();
  return t.fail('it not throw an error');
});

tap.test('createMessage method should create new Message', async (t) => {
  const mongoSQMInstance = await setup();
  const objMsg = {
    a: 'b',
    c: 'd',
  };
  const objCreatedMsg = await mongoSQMInstance.createMessage(objMsg);
  t.match(objCreatedMsg.message, objMsg, 'Created message should match');
  winston.debug('objCreatedMsg %j', objCreatedMsg);
  const objDeletedMsg = await mongoSQMInstance.removeMessageById(objCreatedMsg);
  winston.debug('objDeletedMsg %j', objDeletedMsg);
  await teardown(mongoSQMInstance);
});

tap.test('getMessage method should get some message', async (t) => {
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

  if (arrDeletedMsg.length === testTime) {
    t.pass('It get all message correctly');
  } else {
    t.fail('it got some problem', arrCheck);
  }

  await teardown(mongoSQMInstance);
});

tap.test('getMessage should make messages invisible', async (t) => {
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

  const repeatIn = (ms = 30000) => {
    let countDown = ms;
    return new Promise((resolve) => {
      const timerId = setInterval(async () => {
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
      }, 2500);
    });
  };
  await repeatIn();
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

  if (arrDeletedMsg.length === testTime) {
    t.pass('It get all message correctly');
  } else {
    t.fail('it got some problem', arrCheck);
  }

  await teardown(mongoSQMInstance);
});
