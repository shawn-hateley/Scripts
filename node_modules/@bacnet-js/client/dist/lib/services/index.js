"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.WritePropertyMultiple = exports.WriteProperty = exports.WhoIs = exports.WhoHas = exports.TimeSync = exports.SubscribeProperty = exports.SubscribeCov = exports.ReinitializeDevice = exports.RegisterForeignDevice = exports.ReadRange = exports.ReadPropertyMultiple = exports.ReadProperty = exports.PrivateTransfer = exports.LifeSafetyOperation = exports.IHave = exports.IAm = exports.GetEventInformation = exports.GetEnrollmentSummary = exports.EventNotifyData = exports.EventInformation = exports.ErrorService = exports.DeviceCommunicationControl = exports.DeleteObject = exports.CreateObject = exports.CovNotify = exports.AtomicWriteFile = exports.AtomicReadFile = exports.AlarmSummary = exports.AlarmAcknowledge = exports.AddListElement = exports.BacnetAckService = exports.BacnetService = void 0;
const AbstractServices_1 = require("./AbstractServices");
Object.defineProperty(exports, "BacnetService", { enumerable: true, get: function () { return AbstractServices_1.BacnetService; } });
Object.defineProperty(exports, "BacnetAckService", { enumerable: true, get: function () { return AbstractServices_1.BacnetAckService; } });
const AddListElement_1 = __importDefault(require("./AddListElement"));
exports.AddListElement = AddListElement_1.default;
const AlarmAcknowledge_1 = __importDefault(require("./AlarmAcknowledge"));
exports.AlarmAcknowledge = AlarmAcknowledge_1.default;
const AlarmSummary_1 = __importDefault(require("./AlarmSummary"));
exports.AlarmSummary = AlarmSummary_1.default;
const AtomicReadFile_1 = __importDefault(require("./AtomicReadFile"));
exports.AtomicReadFile = AtomicReadFile_1.default;
const AtomicWriteFile_1 = __importDefault(require("./AtomicWriteFile"));
exports.AtomicWriteFile = AtomicWriteFile_1.default;
const CovNotify_1 = __importDefault(require("./CovNotify"));
exports.CovNotify = CovNotify_1.default;
const CreateObject_1 = __importDefault(require("./CreateObject"));
exports.CreateObject = CreateObject_1.default;
const DeleteObject_1 = __importDefault(require("./DeleteObject"));
exports.DeleteObject = DeleteObject_1.default;
const DeviceCommunicationControl_1 = __importDefault(require("./DeviceCommunicationControl"));
exports.DeviceCommunicationControl = DeviceCommunicationControl_1.default;
const Error_1 = __importDefault(require("./Error"));
exports.ErrorService = Error_1.default;
const EventInformation_1 = __importDefault(require("./EventInformation"));
exports.EventInformation = EventInformation_1.default;
const EventNotifyData_1 = __importDefault(require("./EventNotifyData"));
exports.EventNotifyData = EventNotifyData_1.default;
const GetEnrollmentSummary_1 = __importDefault(require("./GetEnrollmentSummary"));
exports.GetEnrollmentSummary = GetEnrollmentSummary_1.default;
const GetEventInformation_1 = __importDefault(require("./GetEventInformation"));
exports.GetEventInformation = GetEventInformation_1.default;
const IAm_1 = __importDefault(require("./IAm"));
exports.IAm = IAm_1.default;
const IHave_1 = __importDefault(require("./IHave"));
exports.IHave = IHave_1.default;
const LifeSafetyOperation_1 = __importDefault(require("./LifeSafetyOperation"));
exports.LifeSafetyOperation = LifeSafetyOperation_1.default;
const PrivateTransfer_1 = __importDefault(require("./PrivateTransfer"));
exports.PrivateTransfer = PrivateTransfer_1.default;
const ReadProperty_1 = __importDefault(require("./ReadProperty"));
exports.ReadProperty = ReadProperty_1.default;
const ReadPropertyMultiple_1 = __importDefault(require("./ReadPropertyMultiple"));
exports.ReadPropertyMultiple = ReadPropertyMultiple_1.default;
const ReadRange_1 = __importDefault(require("./ReadRange"));
exports.ReadRange = ReadRange_1.default;
const RegisterForeignDevice_1 = __importDefault(require("./RegisterForeignDevice"));
exports.RegisterForeignDevice = RegisterForeignDevice_1.default;
const ReinitializeDevice_1 = __importDefault(require("./ReinitializeDevice"));
exports.ReinitializeDevice = ReinitializeDevice_1.default;
const SubscribeCov_1 = __importDefault(require("./SubscribeCov"));
exports.SubscribeCov = SubscribeCov_1.default;
const SubscribeProperty_1 = __importDefault(require("./SubscribeProperty"));
exports.SubscribeProperty = SubscribeProperty_1.default;
const TimeSync_1 = __importDefault(require("./TimeSync"));
exports.TimeSync = TimeSync_1.default;
const WhoHas_1 = __importDefault(require("./WhoHas"));
exports.WhoHas = WhoHas_1.default;
const WhoIs_1 = __importDefault(require("./WhoIs"));
exports.WhoIs = WhoIs_1.default;
const WriteProperty_1 = __importDefault(require("./WriteProperty"));
exports.WriteProperty = WriteProperty_1.default;
const WritePropertyMultiple_1 = __importDefault(require("./WritePropertyMultiple"));
exports.WritePropertyMultiple = WritePropertyMultiple_1.default;
const ServicesMap = {
    addListElement: AddListElement_1.default,
    alarmAcknowledge: AlarmAcknowledge_1.default,
    alarmSummary: AlarmSummary_1.default,
    atomicReadFile: AtomicReadFile_1.default,
    atomicWriteFile: AtomicWriteFile_1.default,
    covNotify: CovNotify_1.default,
    covNotifyUnconfirmed: CovNotify_1.default,
    createObject: CreateObject_1.default,
    deleteObject: DeleteObject_1.default,
    deviceCommunicationControl: DeviceCommunicationControl_1.default,
    error: Error_1.default,
    eventInformation: EventInformation_1.default,
    eventNotify: EventNotifyData_1.default,
    eventNotifyData: EventNotifyData_1.default,
    getEnrollmentSummary: GetEnrollmentSummary_1.default,
    getEventInformation: GetEventInformation_1.default,
    iAm: IAm_1.default,
    iHave: IHave_1.default,
    lifeSafetyOperation: LifeSafetyOperation_1.default,
    privateTransfer: PrivateTransfer_1.default,
    readProperty: ReadProperty_1.default,
    readPropertyMultiple: ReadPropertyMultiple_1.default,
    readRange: ReadRange_1.default,
    registerForeignDevice: RegisterForeignDevice_1.default,
    reinitializeDevice: ReinitializeDevice_1.default,
    subscribeCov: SubscribeCov_1.default,
    subscribeProperty: SubscribeProperty_1.default,
    timeSync: TimeSync_1.default,
    timeSyncUTC: TimeSync_1.default,
    whoHas: WhoHas_1.default,
    whoIs: WhoIs_1.default,
    writeProperty: WriteProperty_1.default,
    writePropertyMultiple: WritePropertyMultiple_1.default,
};
exports.default = ServicesMap;
//# sourceMappingURL=index.js.map