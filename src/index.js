/* @flow */
import Promise from 'bluebird';
import mongoose from 'mongoose';
import EventEmitter from 'events';
import crypto from 'crypto';

type MongoSMQ$options = {
    host?: string,
    db?: string,
    port?: number,
    options?: any,
    client?: ?string,
    ns?: string,
    visibility?: number,
};

// some helper functions
function id() {
  return crypto.randomBytes(16).toString('hex');
}

function now() {
  return (new Date()).toISOString();
}

function nowPlusSecs(secs: number) {
  return (new Date(Date.now() + (secs * 1000))).toISOString();
}

mongoose.Promise = Promise;

const { Schema } = mongoose;
const MessageSchema = new Schema({
  message: Schema.Types.Mixed,
  visible: Schema.Types.Date,
  ack: Schema.Types.String,
  tries: Schema.Types.Number,
}, {
  timestamps: true,
});
export default class MongoSMQ extends EventEmitter {
  options: MongoSMQ$options;
  mongo: Mongoose$Connection;
  Message: Class<Mongoose$Model>;

  constructor(options: MongoSMQ$options = {}) {
    super();
    const opts = Object.assign({}, {
      host: 'localhost',
      db: 'mongoSMQ',
      port: 27017,
      options: {},
      client: null,
      ns: 'rsmq',
      visibility: 30,
    }, options);
    this.options = opts;
  }

  init(): Promise<MongoSMQ> {
    const { host = '', port = '', db = '' } = this.options;
    return mongoose.connect(
      `mongodb://${host}:${port}/${db}`, {
        useMongoClient: true,
      }).then((connection) => {
      if (connection) {
        this.mongo = connection;
        this.Message = this.mongo.model('SMQMessage', MessageSchema);
      }
      return (this: MongoSMQ);
    });
  }
  deinit(): Promise<Mongoose$Connection> {
    return this.mongo.close();
  }

  createMessage(payload: mixed) {
    const Message = this.Message;
    const newMsg = new Message({
      message: payload,
      visible: now(),
    });
    return newMsg.save().then(obj => obj);
  }

  getMessage(payload: mixed, opts: {visibility: number}): ?Promise<Object> {
    const Message = this.Message;
    const visibility = (opts && opts.visibility) || this.options.visibility;
    const query = {
      deleted: null,
      visible: { $lte: now() },
    };
    const sort = {
      _id: 1,
      visible: 1,
    };
    const update = {
      $inc: { tries: 1 },
      $set: {
        ack: id(),
        visible: nowPlusSecs(visibility || 0),
      },
    };
    return Message.findOneAndUpdate(query, update, { sort, new: true }).then(resp => resp);
  }

  removeMessageById({ _id, ack }: { _id: string, ack: ?string }): Promise<any> {
    const Message = this.Message;
    const query = {
      _id,
      ack,
    };
    /* For ack value,
    ** If it null we mean we looking for object with ack property is null or not exist
    ** If it undefined we mean we don't care about value of ack when find 
    */
    if (ack === undefined) {
      delete query.ack;
    }
    return Message.findOneAndRemove(query).then(resp => resp);
  }
}
