const express = require("express");
const cors = require("cors");
require("dotenv").config();
const ObjectID = require("mongodb").ObjectID;
const bodyParser = require("body-parser");
const _ = require("lodash");
const path = require("path");

const app = express();
app.use(express.static("../sheikh_100s_client/build"));
app.use(cors());
app.use(bodyParser.urlencoded({ extended: false, limit: "5000mb" }));
app.use(bodyParser.json({ limit: "5000mb" }));
const port = 5030;

const MongoClient = require("mongodb").MongoClient;
// const MongoClient = require("mongodb").MongoClient;
// const uri = "mongodb://127.0.0.1:27017/jti_teaser";
const uri =
  "mongodb+srv://aktcl:01939773554op5t@cluster0.9akoo.mongodb.net/jti_teaser?retryWrites=true&w=majority";
const client = new MongoClient(uri, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});
client.connect((err) => {
  const userCollection = client.db("jti_teaser").collection("users");
  const adminCollection = client.db("jti_teaser").collection("admins");
  const leadsCollection = client.db("jti_teaser").collection("leads");
  const baWiseReportCollection = client
    .db("jti_teaser")
    .collection("baWiseReport");
  const territoryWiseReportCollection = client
    .db("jti_teaser")
    .collection("territoryWiseReport");
  const areaWiseReportCollection = client
    .db("jti_teaser")
    .collection("areaWiseReport");
  const regionWiseReportCollection = client
    .db("jti_teaser")
    .collection("regionWiseReport");
  const detailsReportCollection = client
    .db("jti_teaser")
    .collection("detailsReport");
  console.log("user Connection");
  app.get("/agent", (req, res) => {
    const email = req.query.email;
    console.log(email);
    userCollection.find({ email: email }).toArray((err, agents) => {
      console.log(agents[0]);
      res.send(agents[0]);
    });
  });
  app.get("/admin", (req, res) => {
    const email = req.query.email;
    console.log(email);
    adminCollection.find({ email: email }).toArray((err, admins) => {
      console.log(admins[0]);
      res.send(admins[0]);
    });
  });
  app.get("/dMatched/:Consumer_No", (req, res) => {
    const for_d = "d";
    leadsCollection.find({ for_d: for_d }).toArray((err, d) => {
      const Consumer_No = parseInt(req.params.Consumer_No);
      const dNumber = d.find((dOut) => dOut.Consumer_No === Consumer_No);
      console.log(dNumber);
      res.send(dNumber);
    });
  });
  app.patch("/answers/:id", (req, res) => {
    const answers = req.body;
    console.log(answers);
    const id = ObjectID(req.params.id);
    leadsCollection
      .updateOne(
        { _id: id },
        {
          $set: {
            answer1: answers.ans1,
            answer2: answers.ans2,
            answer3: answers.ans3,
            answer4: answers.ans4,
            answer5: answers.ans5,
            answer6: answers.ans6,
            answer7: answers.ans7,
            answer8: answers.ans8,
            agentID: answers.agentID,
            callDate: answers.callDate,
            callTime: answers.callTime,
          },
        }
      )
      .then((result) => {
        console.log(result);
      });
  });
  app.get("/reports", (req, res) => {
    leadsCollection.find({}).toArray((err, reports) => {
      res.send(reports);
    });
  });
  app.get("/qc/:number", (req, res) => {
    const number = req.params.number;
    leadsCollection.find({ Consumer_No: number }).toArray((err, qcs) => {
      console.log(qcs);
      res.send(qcs);
    });
  });
  app.get("/update/:id", (req, res) => {
    const id = req.params.id;
    console.log(id);
    leadsCollection
      .find({ _id: ObjectID(req.params.id) })
      .toArray((err, update) => {
        console.log(update);
        res.send(update);
      });
  });
  app.delete("/deleteAll", (req, res) => {
    leadsCollection.deleteMany({}).then((result) => {
      console.log(result);
      res.send(result.deletedCount > 0);
    });
  });
  app.patch("/finalUpdate/:id", (req, res) => {
    const id = ObjectID(req.params.id);
    const update = req.body;
    console.log(id);
    console.log(update);
    leadsCollection
      .updateOne(
        { _id: id },
        {
          $set: {
            answer1: update.answer1,
            answer2: update.answer2,
            answer3: update.answer3,
            answer4: update.answer4,
            answer5: update.answer5,
            answer6: update.answer6,
            answer7: update.answer7,
            answer8: update.answer8,
            qcChecked: update.qcChecked,
            remarks: update.remarks,
            rating: update.rating,
            qcDate: update.qcDate,
            qcTime: update.qcTime,
          },
        }
      )
      .then((result) => {
        console.log(result);
        res.send(result.modifiedCount > 0);
      });
  });
  app.get("/finalReportLead", (req, res) => {
    reportsCollection.find({}).toArray((err, finalLeads) => {
      console.log(finalLeads);
      res.send(finalLeads);
    });
  });
  app.post("/uploadLead", (req, res) => {
    const leadData = req.body;
    console.log(leadData);
    leadsCollection.insertMany(leadData).then((result) => {
      res.send(result.insertedCount > 0);
    });
  });
  app.post("/adminSignUp", (req, res) => {
    const admin = req.body;
    adminCollection.insertOne(admin).then((result) => {
      res.send(result.insertedCount > 0);
    });
  });
  app.post("/reportsData", (req, res) => {
    const detailsReports = req.body;
    detailsReportCollection.insertMany(detailsReports).then((result) => {
      res.send(result.insertedCount > 0);
    });
  });
  app.get("/reportDates", async (req, res) => {
    async function analyzeData() {
      let result = [];
      try {
        let data = await leadsCollection.find({}).toArray();
        let dates = _.groupBy(JSON.parse(JSON.stringify(data)), function (d) {
          return d.data_date;
        });
        for (date in dates) {
          result.push({
            date: date,
          });
        }
      } catch (e) {
        console.log(e.message);
      }
      res.send(result);
    }
    analyzeData();
  });
  app.get("/prepareByDate", (req, res) => {
    let pDate = req.query;
    console.log(pDate.date);
    leadsCollection.find({ data_date: pDate?.date }).toArray((err, result) => {
      res.send(result);
    });
  });
  app.delete("/deleteByDate", (req, res) => {
    let pDate = req.query;
    console.log(pDate.date);
    leadsCollection.deleteMany({ data_date: pDate.date }).then((result) => {
      res.send(result.deletedCount > 0);
    });
  });
  app.get("/initialLead", (req, res) => {
    let initDate = req.query.initDate;
    console.log(initDate);
    leadsCollection
      .aggregate([
        {
          $match: {
            $and: [
              { for_d: null },
              { Data_Status: "Valid_Data" },
              { data_date: initDate },
            ],
          },
        },
      ])
      .toArray((err, results) => {
        let output = [];
        let users = _.groupBy(
          JSON.parse(JSON.stringify(results)),
          function (d) {
            return d.ba_id;
          }
        );
        for (user in users) {
          output.push({
            // userId: user,
            // consumers: users[user],
            // countByUser: users[user].length,
            stick_sales: users[user]
              .filter(
                (x) =>
                  x.sales_status === "1_Stick_trial" ||
                  x.sales_status === "2-3_Stick_trial"
              )
              .slice(
                0,
                users[user].filter(
                  (x) =>
                    x.sales_status === "Pack_Purchase" ||
                    x.sales_status === "Paper_sachet"
                ).length === 0 ||
                  users[user].filter(
                    (x) =>
                      x.sales_status === "Pack_Purchase" ||
                      x.sales_status === "Paper_sachet"
                  ).length === null ||
                  users[user].filter(
                    (x) =>
                      x.sales_status === "Pack_Purchase" ||
                      x.sales_status === "Paper_sachet"
                  ).length === undefined
                  ? 3
                  : 2
              )
              .map((d) => {
                let datas = {};
                (datas.id = d._id),
                  (datas.ID = d.ID),
                  (datas.data_date = d.data_date),
                  (datas.r_name = d.r_name),
                  (datas.Consumer_No = d.Consumer_No);
                return datas;
              }),
            packet_sales: users[user]
              .filter(
                (x) =>
                  x.sales_status === "Pack_Purchase" ||
                  x.sales_status === "Paper_sachet"
              )
              .slice(
                0,
                users[user].filter(
                  (x) =>
                    x.sales_status === "1_Stick_trial" ||
                    x.sales_status === "2-3_Stick_trial"
                ).length < 2
                  ? 3 -
                      users[user].filter(
                        (x) =>
                          x.sales_status === "1_Stick_trial" ||
                          x.sales_status === "2-3_Stick_trial"
                      ).length
                  : 1
              )
              .map((d) => {
                let datas = {};
                (datas.id = d._id),
                  (datas.ID = d.ID),
                  (datas.data_date = d.data_date),
                  (datas.r_name = d.r_name),
                  (datas.Consumer_No = d.Consumer_No);
                return datas;
              }),
          });
        }
        res.send(output);
      });
  });
  app.patch("/updateInitialLead", async (req, res) => {
    const initialLead = req.body;
    console.log(initialLead);
    let buldOperation = [];
    let counter = 0;

    try {
      initialLead.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element.id) },
            update: {
              $set: {
                for_d: element.for_d,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await leadsCollection.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await leadsCollection.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: true,
      });
    } catch (error) {
      console.log(error);
    }
  });
  app.get("/regenerate", (req, res) => {
    const regenDate = req.query.regenDate;
    console.log(regenDate);
    leadsCollection
      .aggregate([
        {
          $match: {
            $and: [{ Data_Status: "Valid_Data" }, { data_date: regenDate }],
          },
        },
      ])
      .toArray((err, results) => {
        let output = [];
        let users = _.groupBy(
          JSON.parse(JSON.stringify(results)),
          function (d) {
            return d.ba_id;
          }
        );
        for (user in users) {
          output.push({
            // userId: user,
            // consumers: users[user],
            // countByUser: users[user].length,
            // callDone: users[user].filter(
            //   (x) => x.answer10 === "yes" || x.answer10 === "no"
            // ).length,
            new_stick_sales: users[user]
              .filter(
                (x) =>
                  (x.sales_status === "1_Stick_trial" ||
                    x.sales_status === "2-3_Stick_trial") &&
                  x.for_d === null &&
                  (x.answer6 === null || x.answer6 === undefined)
              )
              .slice(
                0,
                users[user].filter((x) => x.answer6 === "yes").length < 2
                  ? 2 - users[user].filter((x) => x.answer6 === "yes").length
                  : 0
              )
              .map((d) => {
                let datas = {};
                (datas.id = d._id),
                  (datas.ID = d.ID),
                  (datas.data_date = d.data_date),
                  (datas.r_name = d.r_name),
                  (datas.Consumer_No = d.Consumer_No);
                return datas;
              }),
            new_packet_sales: users[user]
              .filter(
                (x) =>
                  x.for_d === null &&
                  (x.sales_status === "Pack_Purchase" ||
                    x.sales_status === "Paper_sachet") &&
                  (x.answer6 === null || x.answer6 === undefined)
              )
              .slice(
                0,
                users[user].filter((x) => x.answer6 === "yes").length < 1
                  ? 1
                  : 0
              )
              .map((d) => {
                let datas = {};
                (datas.id = d._id),
                  (datas.ID = d.ID),
                  (datas.data_date = d.data_date),
                  (datas.r_name = d.r_name),
                  (datas.Consumer_No = d.Consumer_No);
                return datas;
              }),
          });
        }
        res.send(output);
      });
  });
  app.patch("/regenerateUpdate", async (req, res) => {
    const regenerateLead = req.body;
    console.log(regenerateLead);
    let buldOperation = [];
    let counter = 0;

    try {
      regenerateLead.forEach(async (element) => {
        buldOperation.push({
          updateOne: {
            filter: { _id: ObjectID(element.id) },
            update: {
              $set: {
                for_d: element.for_d,
              },
            },
          },
        });
        counter++;

        if (counter % 500 == 0) {
          await leadsCollection.bulkWrite(buldOperation);
          buldOperation = [];
        }
      });
      if (counter % 500 != 0) {
        await leadsCollection.bulkWrite(buldOperation);
        buldOperation = [];
      }
      console.log("DONE ================== ");

      res.status(200).json({
        message: true,
      });
    } catch (error) {
      console.log(error);
    }
  });
  app.get("/baReport/:date", async (req, res) => {
    const date = req.params.date;
    console.log(date);
    async function analyzeData() {
      let result = [];
      let total_data_sum = 0;
      let valid_data_sum = 0;
      let valid_data_sum_percentage = 0;
      let dublicate_data_sum = 0;
      let dublicate_data_sum_percentage = 0;
      let error_data_sum = 0;
      let error_data_sum_percentage = 0;
      let total_dial_call_sum = 0;
      let connected_call_sum = 0;
      let connected_call_percentage = 0;
      let not_permitted_to_call_sum = 0;
      let permitted_to_call_sum = 0;
      let call_permission_sum_percentage = 0;
      let bellow_18_sum = 0;
      let non_smoker_sum = 0;
      let ba_did_not_pay_visit_sum = 0;
      let total_fake_call_sum = 0;
      let fake_call_sum_percentage = 0;
      let ba_did_visit_sum = 0;
      let right_franchise_sum = 0;
      let stick_purchase_sum = 0;
      try {
        let data = await leadsCollection.find({ data_date: date }).toArray();
        let users = _.groupBy(JSON.parse(JSON.stringify(data)), function (d) {
          return d.ba_id;
        });
        for (user in users) {
          result.push({
            userId: user,
            date: new Date(users[user][0].data_date),
            baName: users[user][0].BA_Name,
            region: users[user][0].Region,
            area: users[user][0].Area,
            territory: users[user][0].Territory,
            Sales_Point: users[user][0].Sales_Point,
            agencyName: users[user][0].agencyName,
            allocated_target: users[user][0].allocated_target,
            total_data_count:
              users[user].filter((x) => x.Data_Status === "Valid_Data").length +
              users[user].filter((x) => x.Data_Status === "Duplicate_Data")
                .length +
              users[user].filter((x) => x.Data_Status === "Error_Data").length,
            total_data_achived_percentage:
              ((users[user].filter((x) => x.Data_Status === "Valid_Data")
                .length +
                users[user].filter((x) => x.Data_Status === "Duplicate_Data")
                  .length +
                users[user].filter((x) => x.Data_Status === "Error_Data")
                  .length) /
                users[user][0].allocated_target) *
              100,
            valid_Data_count: users[user].filter(
              (x) => x.Data_Status === "Valid_Data"
            ).length,
            dublicate_Data_count: users[user].filter(
              (x) => x.Data_Status === "Duplicate_Data"
            ).length,
            error_Data_count: users[user].filter(
              (x) => x.Data_Status === "Error_Data"
            ).length,
            valid_data_percentage:
              (users[user].filter((x) => x.Data_Status === "Valid_Data")
                .length /
                (users[user].filter((x) => x.Data_Status === "Valid_Data")
                  .length +
                  users[user].filter((x) => x.Data_Status === "Duplicate_Data")
                    .length +
                  users[user].filter((x) => x.Data_Status === "Error_Data")
                    .length)) *
              100,
            dublicate_data_percentage:
              (users[user].filter((x) => x.Data_Status === "Duplicate_Data")
                .length /
                (users[user].filter((x) => x.Data_Status === "Valid_Data")
                  .length +
                  users[user].filter((x) => x.Data_Status === "Duplicate_Data")
                    .length +
                  users[user].filter((x) => x.Data_Status === "Error_Data")
                    .length)) *
              100,
            error_data_percentage:
              (users[user].filter((x) => x.Data_Status === "Error_Data")
                .length /
                (users[user].filter((x) => x.Data_Status === "Valid_Data")
                  .length +
                  users[user].filter((x) => x.Data_Status === "Duplicate_Data")
                    .length +
                  users[user].filter((x) => x.Data_Status === "Error_Data")
                    .length)) *
              100,
            total_dial_call: users[user].filter((x) => x.for_d === "d").length,
            total_connected_call: users[user].filter(
              (x) => x.answer1 === "yes" || x.answer1 === "no"
            ).length,
            total_connected_call_percentage:
              (users[user].filter(
                (x) => x.answer1 === "yes" || x.answer1 === "no"
              ).length /
                users[user].filter((x) => x.for_d === "d").length) *
              100,
            not_permitted_to_call: users[user].filter((x) => x.answer2 === "no")
              .length,
            permitted_to_call: users[user].filter((x) => x.answer2 === "yes")
              .length,
            call_permission_percentage:
              (users[user].filter((x) => x.answer2 === "yes").length /
                users[user].filter(
                  (x) => x.answer1 === "yes" || x.answer1 === "no"
                ).length) *
              100,
            bellow_18: users[user].filter((x) => x.answer3 === "-18").length,
            non_smoker: users[user].filter((x) => x.answer4 === "no").length,
            ba_did_not_pay_visit: users[user].filter((x) => x.answer7 === "no")
              .length,
            total_fake_call:
              users[user].filter((x) => x.answer3 === "-18").length +
              users[user].filter((x) => x.answer4 === "no").length +
              users[user].filter((x) => x.answer7 === "no").length,
            fake_call_percentage:
              ((users[user].filter((x) => x.answer3 === "-18").length +
                users[user].filter((x) => x.answer4 === "no").length +
                users[user].filter((x) => x.answer7 === "no").length) /
                users[user].filter(
                  (x) => x.answer1 === "yes" || x.answer1 === "no"
                ).length) *
              100,
            ba_did_visit: users[user].filter((x) => x.answer7 === "yes").length,
            right_franchise: users[user].filter(
              (x) =>
                x.answer5 === "real" ||
                x.answer5 === "hollywood" ||
                x.answer5 === "derby" ||
                x.answer5 === "royals"
            ).length,
            stick_purchase: users[user].filter((x) => x.answer8dot2 === "yes")
              .length,
          });
          total_data_sum +=
            users[user].filter((x) => x.Data_Status === "Valid_Data").length +
            users[user].filter((x) => x.Data_Status === "Duplicate_Data")
              .length +
            users[user].filter((x) => x.Data_Status === "Error_Data").length;
          valid_data_sum += users[user].filter(
            (x) => x.Data_Status === "Valid_Data"
          ).length;
          valid_data_sum_percentage = parseFloat(
            (valid_data_sum / total_data_sum) * 100
          ).toFixed(2);
          dublicate_data_sum += users[user].filter(
            (x) => x.Data_Status === "Duplicate_Data"
          ).length;
          dublicate_data_sum_percentage = parseFloat(
            (dublicate_data_sum / total_data_sum) * 100
          ).toFixed(2);
          error_data_sum += users[user].filter(
            (x) => x.Data_Status === "Error_Data"
          ).length;
          error_data_sum_percentage = parseFloat(
            (error_data_sum / total_data_sum) * 100
          ).toFixed(2);
          total_dial_call_sum += users[user].filter(
            (x) => x.for_d === "d"
          ).length;
          connected_call_sum += users[user].filter(
            (x) => x.answer1 === "yes" || x.answer1 === "no"
          ).length;
          connected_call_percentage = parseFloat(
            (connected_call_sum / total_dial_call_sum) * 100
          ).toFixed(2);
          not_permitted_to_call_sum += users[user].filter(
            (x) => x.answer2 === "no"
          ).length;
          permitted_to_call_sum += users[user].filter(
            (x) => x.answer2 === "yes"
          ).length;
          call_permission_sum_percentage = parseFloat(
            (permitted_to_call_sum / connected_call_sum) * 100
          ).toFixed(2);
          bellow_18_sum += users[user].filter(
            (x) => x.answer3 === "-18"
          ).length;
          non_smoker_sum += users[user].filter(
            (x) => x.answer4 === "no"
          ).length;
          ba_did_not_pay_visit_sum += users[user].filter(
            (x) => x.answer7 === "no"
          ).length;
          total_fake_call_sum =
            bellow_18_sum + non_smoker_sum + ba_did_not_pay_visit_sum;
          fake_call_sum_percentage = parseFloat(
            (total_fake_call_sum / connected_call_sum) * 100
          ).toFixed(2);
          ba_did_visit_sum += users[user].filter(
            (x) => x.answer7 === "yes"
          ).length;
          right_franchise_sum += users[user].filter(
            (x) =>
              x.answer5 === "real" ||
              x.answer5 === "hollywood" ||
              x.answer5 === "derby" ||
              x.answer5 === "royals"
          ).length;
          stick_purchase_sum += users[user].filter(
            (x) => x.answer8dot2 === "yes"
          ).length;
        }

        result = result.map((r) => {
          return {
            ...r,
            total_data_sum,
            valid_data_sum,
            valid_data_sum_percentage,
            dublicate_data_sum,
            dublicate_data_sum_percentage,
            error_data_sum,
            error_data_sum_percentage,
            total_dial_call_sum,
            connected_call_sum,
            connected_call_percentage,
            not_permitted_to_call_sum,
            permitted_to_call_sum,
            call_permission_sum_percentage,
            bellow_18_sum,
            non_smoker_sum,
            ba_did_not_pay_visit_sum,
            total_fake_call_sum,
            fake_call_sum_percentage,
            ba_did_visit_sum,
            right_franchise_sum,
            stick_purchase_sum,
          };
        });
        console.log("Unique Users", result);
        insertResult(result);
      } catch (e) {
        console.log(e.message);
      }
    }

    async function insertResult(data) {
      try {
        await baWiseReportCollection.insertMany(
          JSON.parse(JSON.stringify(data))
        );
        console.log("Inserted");
        res.send(true);
      } catch (e) {
        // res.send("Error", e.message);
        res.status(404).send("error");
      }
    }
    analyzeData();
  });
  app.get("/territoryReport/:date", async (req, res) => {
    const date = req.params.date;
    console.log(date);
    async function analyzeData() {
      let result = [];
      let total_data_sum = 0;
      let valid_data_sum = 0;
      let valid_data_sum_percentage = 0;
      let dublicate_data_sum = 0;
      let dublicate_data_sum_percentage = 0;
      let error_data_sum = 0;
      let error_data_sum_percentage = 0;
      let total_dial_call_sum = 0;
      let connected_call_sum = 0;
      let connected_call_percentage = 0;
      let not_permitted_to_call_sum = 0;
      let permitted_to_call_sum = 0;
      let call_permission_sum_percentage = 0;
      let bellow_18_sum = 0;
      let non_smoker_sum = 0;
      let ba_did_not_pay_visit_sum = 0;
      let total_fake_call_sum = 0;
      let fake_call_sum_percentage = 0;
      let ba_did_visit_sum = 0;
      let right_franchise_sum = 0;
      let stick_purchase_sum = 0;
      try {
        let data = await baWiseReportCollection
          .find({ date: new Date(date).toISOString() })
          .toArray();
        let users = _.groupBy(JSON.parse(JSON.stringify(data)), function (d) {
          return d.territory;
        });
        for (user in users) {
          result.push({
            userId: user,
            date: new Date(users[user][0].date),
            baName: users[user][0].baName,
            region: users[user][0].region,
            area: users[user][0].area,
            territory: users[user][0].territory,
            Sales_Point: users[user][0].Sales_Point,
            agencyName: users[user][0].agencyName,
            allocated_target: users[user]
              .filter((x) => x.allocated_target)
              .map((x) => Number(x.allocated_target))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_data_count: users[user]
              .filter((x) => x.total_data_count)
              .map((x) => Number(x.total_data_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_data_achived_percentage:
              (users[user]
                .filter((x) => x.total_data_count)
                .map((x) => Number(x.total_data_count))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.allocated_target)
                  .map((x) => Number(x.allocated_target))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            total_valid_data: users[user]
              .filter((x) => x.valid_Data_count)
              .map((x) => Number(x.valid_Data_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_valid_data_percentage:
              (users[user]
                .filter((x) => x.valid_Data_count)
                .map((x) => Number(x.valid_Data_count))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_data_count)
                  .map((x) => Number(x.total_data_count))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            total_dublicate_data: users[user]
              .filter((x) => x.dublicate_Data_count)
              .map((x) => Number(x.dublicate_Data_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_dublicate_data_percentage:
              (users[user]
                .filter((x) => x.dublicate_Data_count)
                .map((x) => Number(x.dublicate_Data_count))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_data_count)
                  .map((x) => Number(x.total_data_count))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            total_error_data: users[user]
              .filter((x) => x.error_Data_count)
              .map((x) => Number(x.error_Data_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_error_data_percentage:
              (users[user]
                .filter((x) => x.error_Data_count)
                .map((x) => Number(x.error_Data_count))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_data_count)
                  .map((x) => Number(x.total_data_count))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            total_dial_call: users[user]
              .filter((x) => x.total_dial_call)
              .map((x) => Number(x.total_dial_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),

            total_connected_call: users[user]
              .filter((x) => x.total_connected_call)
              .map((x) => Number(x.total_connected_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_connected_call_percentage:
              (users[user]
                .filter((x) => x.total_connected_call)
                .map((x) => Number(x.total_connected_call))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_dial_call)
                  .map((x) => Number(x.total_dial_call))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            not_permitted_to_call: users[user]
              .filter((x) => x.not_permitted_to_call)
              .map((x) => Number(x.not_permitted_to_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            permitted_to_call: users[user]
              .filter((x) => x.permitted_to_call)
              .map((x) => Number(x.permitted_to_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            call_permission_percentage:
              (users[user]
                .filter((x) => x.permitted_to_call)
                .map((x) => Number(x.permitted_to_call))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_connected_call)
                  .map((x) => Number(x.total_connected_call))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            bellow_18: users[user]
              .filter((x) => x.bellow_18)
              .map((x) => Number(x.bellow_18))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            non_smoker: users[user]
              .filter((x) => x.non_smoker)
              .map((x) => Number(x.non_smoker))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            ba_did_not_pay_visit: users[user]
              .filter((x) => x.ba_did_not_pay_visit)
              .map((x) => Number(x.ba_did_not_pay_visit))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_fake_call: users[user]
              .filter((x) => x.total_fake_call)
              .map((x) => Number(x.total_fake_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            fake_call_percentage:
              (users[user]
                .filter((x) => x.total_fake_call)
                .map((x) => Number(x.total_fake_call))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_connected_call)
                  .map((x) => Number(x.total_connected_call))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            ba_did_visit: users[user]
              .filter((x) => x.ba_did_visit)
              .map((x) => Number(x.ba_did_visit))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            right_franchise: users[user]
              .filter((x) => x.right_franchise)
              .map((x) => Number(x.right_franchise))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            stick_purchase: users[user]
              .filter((x) => x.stick_purchase)
              .map((x) => Number(x.stick_purchase))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
          });
          total_data_sum += users[user]
            .filter((x) => x.valid_Data_count)
            .map((x) => Number(x.valid_Data_count))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          valid_data_sum += users[user]
            .filter((x) => x.valid_Data_count)
            .map((x) => Number(x.valid_Data_count))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          valid_data_sum_percentage = parseFloat(
            (valid_data_sum / total_data_sum) * 100
          ).toFixed(2);
          dublicate_data_sum += users[user]
            .filter((x) => x.dublicate_Data_count)
            .map((x) => Number(x.dublicate_Data_count))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          dublicate_data_sum_percentage = parseFloat(
            (dublicate_data_sum / total_data_sum) * 100
          ).toFixed(2);
          error_data_sum += users[user]
            .filter((x) => x.error_Data_count)
            .map((x) => Number(x.error_Data_count))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          error_data_sum_percentage = parseFloat(
            (error_data_sum / total_data_sum) * 100
          ).toFixed(2);
          total_dial_call_sum += users[user]
            .filter((x) => x.total_dial_call)
            .map((x) => Number(x.total_dial_call))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          connected_call_sum += users[user]
            .filter((x) => x.total_connected_call)
            .map((x) => Number(x.total_connected_call))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          connected_call_percentage = parseFloat(
            (connected_call_sum / total_dial_call_sum) * 100
          ).toFixed(2);
          not_permitted_to_call_sum += users[user]
            .filter((x) => x.not_permitted_to_call)
            .map((x) => Number(x.not_permitted_to_call))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          permitted_to_call_sum += users[user]
            .filter((x) => x.permitted_to_call)
            .map((x) => Number(x.permitted_to_call))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          call_permission_sum_percentage = parseFloat(
            (permitted_to_call_sum / connected_call_sum) * 100
          ).toFixed(2);
          bellow_18_sum += users[user]
            .filter((x) => x.bellow_18)
            .map((x) => Number(x.bellow_18))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          non_smoker_sum += users[user]
            .filter((x) => x.non_smoker)
            .map((x) => Number(x.non_smoker))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          ba_did_not_pay_visit_sum += users[user]
            .filter((x) => x.ba_did_not_pay_visit)
            .map((x) => Number(x.ba_did_not_pay_visit))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          total_fake_call_sum =
            bellow_18_sum + non_smoker_sum + ba_did_not_pay_visit_sum;
          fake_call_sum_percentage = parseFloat(
            (total_fake_call_sum / connected_call_sum) * 100
          ).toFixed(2);
          ba_did_visit_sum += users[user]
            .filter((x) => x.ba_did_visit)
            .map((x) => Number(x.ba_did_visit))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          right_franchise_sum += users[user]
            .filter((x) => x.right_franchise)
            .map((x) => Number(x.right_franchise))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          stick_purchase_sum += users[user]
            .filter((x) => x.stick_purchase)
            .map((x) => Number(x.stick_purchase))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
        }

        result = result.map((r) => {
          return {
            ...r,
            total_data_sum,
            valid_data_sum,
            valid_data_sum_percentage,
            dublicate_data_sum,
            dublicate_data_sum_percentage,
            error_data_sum,
            error_data_sum_percentage,
            total_dial_call_sum,
            connected_call_sum,
            connected_call_percentage,
            not_permitted_to_call_sum,
            permitted_to_call_sum,
            call_permission_sum_percentage,
            bellow_18_sum,
            non_smoker_sum,
            ba_did_not_pay_visit_sum,
            total_fake_call_sum,
            fake_call_sum_percentage,
            ba_did_visit_sum,
            right_franchise_sum,
            stick_purchase_sum,
          };
        });
        console.log("Unique Users", result);
        insertResult(result);
      } catch (e) {
        console.log(e.message);
      }
    }

    async function insertResult(data) {
      try {
        await territoryWiseReportCollection.insertMany(
          JSON.parse(JSON.stringify(data))
        );
        console.log("Inserted");
        res.send(true);
      } catch (e) {
        // res.send("Error", e.message);
        res.status(404).send("error");
      }
    }
    analyzeData();
  });
  app.get("/areaReport/:date", async (req, res) => {
    const date = req.params.date;
    console.log(date);
    async function analyzeData() {
      let result = [];
      let total_allocated_sum = 0;
      let total_data_sum = 0;
      let valid_data_sum = 0;
      let valid_data_sum_percentage = 0;
      let dublicate_data_sum = 0;
      let dublicate_data_sum_percentage = 0;
      let error_data_sum = 0;
      let error_data_sum_percentage = 0;
      let total_dial_call_sum = 0;
      let connected_call_sum = 0;
      let connected_call_percentage = 0;
      let not_permitted_to_call_sum = 0;
      let permitted_to_call_sum = 0;
      let call_permission_sum_percentage = 0;
      let bellow_18_sum = 0;
      let non_smoker_sum = 0;
      let ba_did_not_pay_visit_sum = 0;
      let total_fake_call_sum = 0;
      let fake_call_sum_percentage = 0;
      let ba_did_visit_sum = 0;
      let right_franchise_sum = 0;
      let stick_purchase_sum = 0;
      try {
        let data = await territoryWiseReportCollection
          .find({ date: new Date(date).toISOString() })
          .toArray();
        let users = _.groupBy(JSON.parse(JSON.stringify(data)), function (d) {
          return d.area;
        });
        for (user in users) {
          result.push({
            userId: user,
            date: new Date(users[user][0].date),
            baName: users[user][0].baName,
            region: users[user][0].region,
            area: users[user][0].area,
            territory: users[user][0].territory,
            Sales_Point: users[user][0].Sales_Point,
            agencyName: users[user][0].agencyName,
            allocated_target: users[user]
              .filter((x) => x.allocated_target)
              .map((x) => Number(x.allocated_target))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_data_count: users[user]
              .filter((x) => x.total_data_count)
              .map((x) => Number(x.total_data_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_data_achived_percentage:
              (users[user]
                .filter((x) => x.total_data_count)
                .map((x) => Number(x.total_data_count))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.allocated_target)
                  .map((x) => Number(x.allocated_target))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            total_valid_data: users[user]
              .filter((x) => x.total_valid_data)
              .map((x) => Number(x.total_valid_data))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_valid_data_percentage:
              (users[user]
                .filter((x) => x.total_valid_data)
                .map((x) => Number(x.total_valid_data))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_data_count)
                  .map((x) => Number(x.total_data_count))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            total_dublicate_data: users[user]
              .filter((x) => x.total_dublicate_data)
              .map((x) => Number(x.total_dublicate_data))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_dublicate_data_percentage:
              (users[user]
                .filter((x) => x.total_dublicate_data)
                .map((x) => Number(x.total_dublicate_data))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_data_count)
                  .map((x) => Number(x.total_data_count))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            total_error_data: users[user]
              .filter((x) => x.total_error_data)
              .map((x) => Number(x.total_error_data))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_error_data_percentage:
              (users[user]
                .filter((x) => x.total_error_data)
                .map((x) => Number(x.total_error_data))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_data_count)
                  .map((x) => Number(x.total_data_count))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            total_dial_call: users[user]
              .filter((x) => x.total_dial_call)
              .map((x) => Number(x.total_dial_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),

            total_connected_call: users[user]
              .filter((x) => x.total_connected_call)
              .map((x) => Number(x.total_connected_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_connected_call_percentage:
              (users[user]
                .filter((x) => x.total_connected_call)
                .map((x) => Number(x.total_connected_call))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_dial_call)
                  .map((x) => Number(x.total_dial_call))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            not_permitted_to_call: users[user]
              .filter((x) => x.not_permitted_to_call)
              .map((x) => Number(x.not_permitted_to_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            permitted_to_call: users[user]
              .filter((x) => x.permitted_to_call)
              .map((x) => Number(x.permitted_to_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            call_permission_percentage:
              (users[user]
                .filter((x) => x.permitted_to_call)
                .map((x) => Number(x.permitted_to_call))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_connected_call)
                  .map((x) => Number(x.total_connected_call))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            bellow_18: users[user]
              .filter((x) => x.bellow_18)
              .map((x) => Number(x.bellow_18))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            non_smoker: users[user]
              .filter((x) => x.non_smoker)
              .map((x) => Number(x.non_smoker))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            ba_did_not_pay_visit: users[user]
              .filter((x) => x.ba_did_not_pay_visit)
              .map((x) => Number(x.ba_did_not_pay_visit))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_fake_call: users[user]
              .filter((x) => x.total_fake_call)
              .map((x) => Number(x.total_fake_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            fake_call_percentage:
              (users[user]
                .filter((x) => x.total_fake_call)
                .map((x) => Number(x.total_fake_call))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_connected_call)
                  .map((x) => Number(x.total_connected_call))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            ba_did_visit: users[user]
              .filter((x) => x.ba_did_visit)
              .map((x) => Number(x.ba_did_visit))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            right_franchise: users[user]
              .filter((x) => x.right_franchise)
              .map((x) => Number(x.right_franchise))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            stick_purchase: users[user]
              .filter((x) => x.stick_purchase)
              .map((x) => Number(x.stick_purchase))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
          });
          total_allocated_sum += users[user]
            .filter((x) => x.allocated_target)
            .map((x) => Number(x.allocated_target))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          total_data_sum += users[user]
            .filter((x) => x.total_data_count)
            .map((x) => Number(x.total_data_count))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          valid_data_sum += users[user]
            .filter((x) => x.total_valid_data)
            .map((x) => Number(x.total_valid_data))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          valid_data_sum_percentage = parseFloat(
            (valid_data_sum / total_data_sum) * 100
          ).toFixed(2);
          dublicate_data_sum += users[user]
            .filter((x) => x.total_dublicate_data)
            .map((x) => Number(x.total_dublicate_data))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          dublicate_data_sum_percentage = parseFloat(
            (dublicate_data_sum / total_data_sum) * 100
          ).toFixed(2);
          error_data_sum += users[user]
            .filter((x) => x.total_error_data)
            .map((x) => Number(x.total_error_data))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          error_data_sum_percentage = parseFloat(
            (error_data_sum / total_data_sum) * 100
          ).toFixed(2);
          total_dial_call_sum += users[user]
            .filter((x) => x.total_dial_call)
            .map((x) => Number(x.total_dial_call))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          connected_call_sum += users[user]
            .filter((x) => x.total_connected_call)
            .map((x) => Number(x.total_connected_call))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          connected_call_percentage = parseFloat(
            (connected_call_sum / total_dial_call_sum) * 100
          ).toFixed(2);
          not_permitted_to_call_sum += users[user]
            .filter((x) => x.not_permitted_to_call)
            .map((x) => Number(x.not_permitted_to_call))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          permitted_to_call_sum += users[user]
            .filter((x) => x.permitted_to_call)
            .map((x) => Number(x.permitted_to_call))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          call_permission_sum_percentage = parseFloat(
            (permitted_to_call_sum / connected_call_sum) * 100
          ).toFixed(2);
          bellow_18_sum += users[user]
            .filter((x) => x.bellow_18)
            .map((x) => Number(x.bellow_18))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          non_smoker_sum += users[user]
            .filter((x) => x.non_smoker)
            .map((x) => Number(x.non_smoker))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          ba_did_not_pay_visit_sum += users[user]
            .filter((x) => x.ba_did_not_pay_visit)
            .map((x) => Number(x.ba_did_not_pay_visit))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          total_fake_call_sum =
            bellow_18_sum + non_smoker_sum + ba_did_not_pay_visit_sum;
          fake_call_sum_percentage = parseFloat(
            (total_fake_call_sum / connected_call_sum) * 100
          ).toFixed(2);
          ba_did_visit_sum += users[user]
            .filter((x) => x.ba_did_visit)
            .map((x) => Number(x.ba_did_visit))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          right_franchise_sum += users[user]
            .filter((x) => x.right_franchise)
            .map((x) => Number(x.right_franchise))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          stick_purchase_sum += users[user]
            .filter((x) => x.stick_purchase)
            .map((x) => Number(x.stick_purchase))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
        }

        result = result.map((r) => {
          return {
            ...r,
            total_allocated_sum,
            total_data_sum,
            valid_data_sum,
            valid_data_sum_percentage,
            dublicate_data_sum,
            dublicate_data_sum_percentage,
            error_data_sum,
            error_data_sum_percentage,
            total_dial_call_sum,
            connected_call_sum,
            connected_call_percentage,
            not_permitted_to_call_sum,
            permitted_to_call_sum,
            call_permission_sum_percentage,
            bellow_18_sum,
            non_smoker_sum,
            ba_did_not_pay_visit_sum,
            total_fake_call_sum,
            fake_call_sum_percentage,
            ba_did_visit_sum,
            right_franchise_sum,
            stick_purchase_sum,
          };
        });
        console.log("Unique Users", result);
        insertResult(result);
      } catch (e) {
        console.log(e.message);
      }
    }

    async function insertResult(data) {
      try {
        await areaWiseReportCollection.insertMany(
          JSON.parse(JSON.stringify(data))
        );
        console.log("Inserted");
        res.send(true);
      } catch (e) {
        // res.send("Error", e.message);
        res.status(404).send("error");
      }
    }
    analyzeData();
  });
  app.get("/regionReport/:date", async (req, res) => {
    const date = req.params.date;
    console.log(date);
    async function analyzeData() {
      let result = [];
      let total_allocated_sum = 0;
      let total_data_sum = 0;
      let valid_data_sum = 0;
      let valid_data_sum_percentage = 0;
      let dublicate_data_sum = 0;
      let dublicate_data_sum_percentage = 0;
      let error_data_sum = 0;
      let error_data_sum_percentage = 0;
      let total_dial_call_sum = 0;
      let connected_call_sum = 0;
      let connected_call_percentage = 0;
      let not_permitted_to_call_sum = 0;
      let permitted_to_call_sum = 0;
      let call_permission_sum_percentage = 0;
      let bellow_18_sum = 0;
      let non_smoker_sum = 0;
      let ba_did_not_pay_visit_sum = 0;
      let total_fake_call_sum = 0;
      let fake_call_sum_percentage = 0;
      let ba_did_visit_sum = 0;
      let right_franchise_sum = 0;
      let stick_purchase_sum = 0;
      try {
        let data = await areaWiseReportCollection
          .find({ date: new Date(date).toISOString() })
          .toArray();
        let users = _.groupBy(JSON.parse(JSON.stringify(data)), function (d) {
          return d.region;
        });
        for (user in users) {
          result.push({
            userId: user,
            date: new Date(users[user][0].date),
            baName: users[user][0].baName,
            region: users[user][0].region,
            area: users[user][0].area,
            territory: users[user][0].territory,
            Sales_Point: users[user][0].Sales_Point,
            agencyName: users[user][0].agencyName,
            allocated_target: users[user]
              .filter((x) => x.allocated_target)
              .map((x) => Number(x.allocated_target))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_data_count: users[user]
              .filter((x) => x.total_data_count)
              .map((x) => Number(x.total_data_count))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_data_achived_percentage:
              (users[user]
                .filter((x) => x.total_data_count)
                .map((x) => Number(x.total_data_count))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.allocated_target)
                  .map((x) => Number(x.allocated_target))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            total_valid_data: users[user]
              .filter((x) => x.total_valid_data)
              .map((x) => Number(x.total_valid_data))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_valid_data_percentage:
              (users[user]
                .filter((x) => x.total_valid_data)
                .map((x) => Number(x.total_valid_data))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_data_count)
                  .map((x) => Number(x.total_data_count))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            total_dublicate_data: users[user]
              .filter((x) => x.total_dublicate_data)
              .map((x) => Number(x.total_dublicate_data))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_dublicate_data_percentage:
              (users[user]
                .filter((x) => x.total_dublicate_data)
                .map((x) => Number(x.total_dublicate_data))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_data_count)
                  .map((x) => Number(x.total_data_count))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            total_error_data: users[user]
              .filter((x) => x.total_error_data)
              .map((x) => Number(x.total_error_data))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_error_data_percentage:
              (users[user]
                .filter((x) => x.total_error_data)
                .map((x) => Number(x.total_error_data))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_data_count)
                  .map((x) => Number(x.total_data_count))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            total_dial_call: users[user]
              .filter((x) => x.total_dial_call)
              .map((x) => Number(x.total_dial_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),

            total_connected_call: users[user]
              .filter((x) => x.total_connected_call)
              .map((x) => Number(x.total_connected_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_connected_call_percentage:
              (users[user]
                .filter((x) => x.total_connected_call)
                .map((x) => Number(x.total_connected_call))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_dial_call)
                  .map((x) => Number(x.total_dial_call))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            not_permitted_to_call: users[user]
              .filter((x) => x.not_permitted_to_call)
              .map((x) => Number(x.not_permitted_to_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            permitted_to_call: users[user]
              .filter((x) => x.permitted_to_call)
              .map((x) => Number(x.permitted_to_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            call_permission_percentage:
              (users[user]
                .filter((x) => x.permitted_to_call)
                .map((x) => Number(x.permitted_to_call))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_connected_call)
                  .map((x) => Number(x.total_connected_call))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            bellow_18: users[user]
              .filter((x) => x.bellow_18)
              .map((x) => Number(x.bellow_18))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            non_smoker: users[user]
              .filter((x) => x.non_smoker)
              .map((x) => Number(x.non_smoker))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            ba_did_not_pay_visit: users[user]
              .filter((x) => x.ba_did_not_pay_visit)
              .map((x) => Number(x.ba_did_not_pay_visit))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            total_fake_call: users[user]
              .filter((x) => x.total_fake_call)
              .map((x) => Number(x.total_fake_call))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            fake_call_percentage:
              (users[user]
                .filter((x) => x.total_fake_call)
                .map((x) => Number(x.total_fake_call))
                .reduce((sum, cv) => (sum += Number(cv)), 0) /
                users[user]
                  .filter((x) => x.total_connected_call)
                  .map((x) => Number(x.total_connected_call))
                  .reduce((sum, cv) => (sum += Number(cv)), 0)) *
              100,
            ba_did_visit: users[user]
              .filter((x) => x.ba_did_visit)
              .map((x) => Number(x.ba_did_visit))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            right_franchise: users[user]
              .filter((x) => x.right_franchise)
              .map((x) => Number(x.right_franchise))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
            stick_purchase: users[user]
              .filter((x) => x.stick_purchase)
              .map((x) => Number(x.stick_purchase))
              .reduce((sum, cv) => (sum += Number(cv)), 0),
          });
          total_allocated_sum += users[user]
            .filter((x) => x.allocated_target)
            .map((x) => Number(x.allocated_target))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          total_data_sum += users[user]
            .filter((x) => x.total_data_count)
            .map((x) => Number(x.total_data_count))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          valid_data_sum += users[user]
            .filter((x) => x.total_valid_data)
            .map((x) => Number(x.total_valid_data))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          valid_data_sum_percentage = parseFloat(
            (valid_data_sum / total_data_sum) * 100
          ).toFixed(2);
          dublicate_data_sum += users[user]
            .filter((x) => x.total_dublicate_data)
            .map((x) => Number(x.total_dublicate_data))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          dublicate_data_sum_percentage = parseFloat(
            (dublicate_data_sum / total_data_sum) * 100
          ).toFixed(2);
          error_data_sum += users[user]
            .filter((x) => x.total_error_data)
            .map((x) => Number(x.total_error_data))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          error_data_sum_percentage = parseFloat(
            (error_data_sum / total_data_sum) * 100
          ).toFixed(2);
          total_dial_call_sum += users[user]
            .filter((x) => x.total_dial_call)
            .map((x) => Number(x.total_dial_call))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          connected_call_sum += users[user]
            .filter((x) => x.total_connected_call)
            .map((x) => Number(x.total_connected_call))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          connected_call_percentage = parseFloat(
            (connected_call_sum / total_dial_call_sum) * 100
          ).toFixed(2);
          not_permitted_to_call_sum += users[user]
            .filter((x) => x.not_permitted_to_call)
            .map((x) => Number(x.not_permitted_to_call))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          permitted_to_call_sum += users[user]
            .filter((x) => x.permitted_to_call)
            .map((x) => Number(x.permitted_to_call))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          call_permission_sum_percentage = parseFloat(
            (permitted_to_call_sum / connected_call_sum) * 100
          ).toFixed(2);
          bellow_18_sum += users[user]
            .filter((x) => x.bellow_18)
            .map((x) => Number(x.bellow_18))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          non_smoker_sum += users[user]
            .filter((x) => x.non_smoker)
            .map((x) => Number(x.non_smoker))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          ba_did_not_pay_visit_sum += users[user]
            .filter((x) => x.ba_did_not_pay_visit)
            .map((x) => Number(x.ba_did_not_pay_visit))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          total_fake_call_sum =
            bellow_18_sum + non_smoker_sum + ba_did_not_pay_visit_sum;
          fake_call_sum_percentage = parseFloat(
            (total_fake_call_sum / connected_call_sum) * 100
          ).toFixed(2);
          ba_did_visit_sum += users[user]
            .filter((x) => x.ba_did_visit)
            .map((x) => Number(x.ba_did_visit))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          right_franchise_sum += users[user]
            .filter((x) => x.right_franchise)
            .map((x) => Number(x.right_franchise))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
          stick_purchase_sum += users[user]
            .filter((x) => x.stick_purchase)
            .map((x) => Number(x.stick_purchase))
            .reduce((sum, cv) => (sum += Number(cv)), 0);
        }

        result = result.map((r) => {
          return {
            ...r,
            total_allocated_sum,
            total_data_sum,
            valid_data_sum,
            valid_data_sum_percentage,
            dublicate_data_sum,
            dublicate_data_sum_percentage,
            error_data_sum,
            error_data_sum_percentage,
            total_dial_call_sum,
            connected_call_sum,
            connected_call_percentage,
            not_permitted_to_call_sum,
            permitted_to_call_sum,
            call_permission_sum_percentage,
            bellow_18_sum,
            non_smoker_sum,
            ba_did_not_pay_visit_sum,
            total_fake_call_sum,
            fake_call_sum_percentage,
            ba_did_visit_sum,
            right_franchise_sum,
            stick_purchase_sum,
          };
        });
        console.log("Unique Users", result);
        insertResult(result);
      } catch (e) {
        console.log(e.message);
      }
    }

    async function insertResult(data) {
      try {
        await regionWiseReportCollection.insertMany(
          JSON.parse(JSON.stringify(data))
        );
        console.log("Inserted");
        res.send(true);
      } catch (e) {
        res.status(404).send("error");
      }
    }
    analyzeData();
  });
  app.get("/getBRReport/:date", (req, res) => {
    const date = req.params.date;
    console.log(date);
    baWiseReportCollection
      .find({ date: new Date(date).toISOString() })
      .toArray((err, results) => {
        res.send(results);
      });
  });
  app.get("/getTerritoryReport/:date", (req, res) => {
    const date = req.params.date;
    console.log(date);
    territoryWiseReportCollection
      .find({ date: new Date(date).toISOString() })
      .toArray((err, results) => {
        res.send(results);
      });
  });
  app.get("/getAreaReport/:date", (req, res) => {
    const date = req.params.date;
    console.log(date);
    areaWiseReportCollection
      .find({ date: new Date(date).toISOString() })
      .toArray((err, results) => {
        res.send(results);
      });
  });
  app.get("/getRegionReport/:date", (req, res) => {
    const date = req.params.date;
    console.log(date);
    regionWiseReportCollection
      .find({ date: new Date(date).toISOString() })
      .toArray((err, results) => {
        res.send(results);
      });
  });
  // app.get("*", (req, res) => {
  //   res.sendFile(
  //     path.join(__dirname, "../sheikh_100s_client/build", "index.html")
  //   );
  // });
});

app.get("/", (req, res) => {
  res.send("Hello World!");
});

app.listen(process.env.PORT || port);
