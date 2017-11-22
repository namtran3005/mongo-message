import * as BPromise from "bluebird";
import * as mongoose from "mongoose";

(mongoose as any).Promise = BPromise;

export interface IMongoSMQ$options {
    host?: string;
    db?: string;
    port?: number;
    options?: any;
    client?: string;
    ns?: string;
    visibility?: number;
    colName?: string;
}

export interface IDocumentMessage extends mongoose.Document {
  _id: {};
  tries?: number;
  message: any;
  visible: any;
}

export interface IHotFixModel extends mongoose.Model<IDocumentMessage> {
  deleteMany(conditions: {}): Promise<{}>;
}

export interface IMongoSMQ$updatePayload {
  _id: string;
  tries?: number;
  message: any;
}

function now() {
  return (new Date()).toISOString();
}

function nowPlusSecs(secs: number) {
  return (new Date(Date.now() + (secs * 1000))).toISOString();
}

const MessageSchema = new mongoose.Schema({
  ack: mongoose.Schema.Types.String,
  message: mongoose.Schema.Types.Mixed,
  tries: mongoose.Schema.Types.Number,
  visible: mongoose.Schema.Types.Date,
}, {
  timestamps: true,
});

export default class MongoSMQ {
  public options: IMongoSMQ$options;
  public mongo: mongoose.Connection;
  public Message: IHotFixModel;

  constructor(options?: IMongoSMQ$options) {
    const opts = Object.assign({}, {
      host: "localhost",
      db: "mongoSMQ",
      port: 27017,
      options: {},
      client: null,
      ns: "rsmq",
      visibility: 30,
      colName: "SMQMessage",
    }, options);
    this.options = opts;
  }

  public async init() {
    const {
      host = "", port = "", db = "", colName,
    } = this.options;
    const theConnection = await mongoose.createConnection(
      `mongodb://${host}:${port}/${db}`,
      {
        useMongoClient: true,
      },
    );
    if (theConnection) {
      this.mongo = theConnection;
      this.Message = this.mongo.model<IDocumentMessage>(colName, MessageSchema) as IHotFixModel;
    }
    return this;
  }

  public deinit(): Promise<void> {
    return this.mongo.close();
  }

  public createMessage(payload?: any): Promise<IDocumentMessage> {
    const { Message } = this;
    const newMsg = new Message({
      message: payload,
      visible: now(),
    });
    return newMsg.save() as Promise<IDocumentMessage>;
  }

  public getMessage(payload?: any, opts?: {visibility: number}): Promise<IDocumentMessage> {
    const { Message } = this;
    const visibility = (opts && opts.visibility !== undefined)
      ? opts.visibility : this.options.visibility;
    const query = Object.assign({
      visible: { $lte: now() },
    }, payload);
    const sort = {
      _id: 1,
      visible: 1,
    };
    const update = {
      $inc: { tries: 1 },
      $set: {
        visible: nowPlusSecs(visibility || 0),
      },
    };
    return Message.findOneAndUpdate(query, update, { sort, new: true }).then();
  }

  public updateMessage(query: {_id: string, tries?: number}, update: any): Promise<IDocumentMessage> {
    const { Message } = this;
    return Message.findOneAndUpdate(query, update, { new: true }).then();
  }

  public removeMessageById({ _id, tries }: { _id: string, tries?: number }): Promise<{}> {
    const { Message } = this;
    const query = {
      _id,
      tries,
    };
    /* For tries value,
    ** If it null we mean we looking for object with tries property is null or not exist
    ** If it undefined we mean we don't care about value of tries when find
    */
    if (tries === undefined) {
      delete query.tries;
    }
    return Message.findOneAndRemove(query).then();
  }

  public total(): Promise<{}> {
    const { Message } = this;
    return Message.count({}).then();
  }

  public size(): Promise<{}> {
    const { Message } = this;
    const query = {
      visible: { $lte: now() },
    };
    return Message.count(query).then();
  }

  public inFlight(): Promise<{}> {
    const { Message } = this;
    const query = {
      tries: { $exists: true },
      visible: { $gt: now() },
    };
    return Message.count(query).then();
  }

  public clean(): Promise<{}> {
    const { Message } = this;
    return Message.deleteMany({}).then();
  }
}
