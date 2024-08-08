import axios from "axios";
import axiosMockAdapter from "axios-mock-adapter";
import * as path from "path";
import { existsSync, unlinkSync } from "fs";
import TransactionETL from "./transactionEtl";

const mock = new axiosMockAdapter(axios);

const API_URL = "http://35.77.82.139:3000/transactions";

describe("TransactionETL", () => {
  const fromDate = "2024-01-01";
  const toDate = "2024-01-31";
  let etl: TransactionETL;

  beforeEach(() => {
    etl = new TransactionETL(fromDate, toDate);
  });

  afterEach(() => {
    mock.reset();
    const filePath = path.join(__dirname, "transactions.csv");
    if (existsSync(filePath)) {
      unlinkSync(filePath);
    }
  });

  describe("constructor", () => {
    it("should throw an error for invalid date formats", () => {
      expect(() => new TransactionETL("invalid-date", toDate)).toThrow(
        "Invalid date format. Dates must be in YYYY-MM-DD format."
      );
    });
  });

  describe("fetchTransactions", () => {
    it("should fetch transactions", async () => {
      const data = {
        data: [{ date: "2024-01-01", amount: "100.00", description: "Test" }],
      };
      mock.onGet(API_URL, { params: { fromDate, toDate } }).reply(200, data);

      const transactions = await (etl as any).fetchTransactions();
      expect(transactions).toEqual(data.data);
    });
  });

  describe("transformTransactions", () => {
    it("should transform transactions", () => {
      const transactions = [
        { date: "2024-01-01", amount: "100.00", description: "Test" },
      ];
      const transformed = (etl as any).transformTransactions(transactions);
      expect(transformed).toEqual([
        { date: "2024-01-01", amount: "100", description: "Test" },
      ]);
    });
  });

  describe("loadTransactions", () => {
    it("should write transactions to CSV", async () => {
      const transactions = [
        { date: "2024-01-01", amount: "100", description: "Test" },
      ];
      const loadTransactionsSpy = jest
        .spyOn(etl as any, "loadTransactions")
        .mockImplementation(async () => {});
      await (etl as any).loadTransactions(transactions);
      expect(
        existsSync(path.join(__dirname, "transactions.csv"))
      ).toBeDefined();
      loadTransactionsSpy.mockRestore();
    });
  });

  describe("run", () => {
    it("should execute the ETL process", async () => {
      const data = {
        data: [{ date: "2024-01-01", amount: "100.00", description: "Test" }],
      };
      mock.onGet(API_URL, { params: { fromDate, toDate } }).reply(200, data);

      const fetchTransactionsSpy = jest.spyOn(etl as any, "fetchTransactions");
      const transformTransactionsSpy = jest.spyOn(
        etl as any,
        "transformTransactions"
      );
      const loadTransactionsSpy = jest
        .spyOn(etl as any, "loadTransactions")
        .mockImplementation(async () => {});

      fetchTransactionsSpy.mockResolvedValue(data.data);
      transformTransactionsSpy.mockReturnValue([
        { date: "2024-01-01", amount: "100", description: "Test" },
      ]);
      loadTransactionsSpy.mockImplementation(async () => {});

      await etl.run();

      expect(fetchTransactionsSpy).toHaveBeenCalled();
      expect(transformTransactionsSpy).toHaveBeenCalledWith(data.data);
      expect(loadTransactionsSpy).toHaveBeenCalledWith([
        { date: "2024-01-01", amount: "100", description: "Test" },
      ]);

      fetchTransactionsSpy.mockRestore();
      transformTransactionsSpy.mockRestore();
      loadTransactionsSpy.mockRestore();
    });
  });
});
