import axios from "axios";
import { createObjectCsvWriter as createCsvWriter } from "csv-writer";
import * as path from "path";

// External api url
const API_URL = "http://35.77.82.139:3000/transactions";

// csv file
const OUTPUT_FILE = path.join(__dirname, "transactions.csv");

const csvWriter = createCsvWriter({
  path: OUTPUT_FILE,
  header: [
    { id: "date", title: "Date" },
    { id: "amount", title: "Amount" },
    { id: "description", title: "Description" },
  ],
});

interface Transaction {
  date: string;
  amount: string;
  description: string;
}

interface AxiosError extends Error {
  response?: {
    status: number;
    message: string;
  };
}

export default class TransactionETL {
  fromDate: string;
  toDate: string;

  constructor(fromDate: string, toDate: string) {
    if (!this.isValidDateFormat(fromDate) || !this.isValidDateFormat(toDate)) {
      throw new Error(
        "Invalid date format. Dates must be in YYYY-MM-DD format."
      );
    }

    this.fromDate = fromDate;
    this.toDate = toDate;
  }

  // validating query string for date
  private isValidDateFormat(date: string): boolean {
    const regex = /^\d{4}-\d{2}-\d{2}$/;
    return regex.test(date);
  }

  private async fetchTransactions(): Promise<Transaction[]> {
    try {
      const response = await axios.get(API_URL, {
        params: {
          fromDate: this.fromDate,
          toDate: this.toDate,
        },
      });

      return response.data.data;
    } catch (error) {
      const axiosError = error as AxiosError;
      if (axiosError.response && axiosError.response.status === 400) {
        console.error(axiosError.response.message, this.fromDate, this.toDate);
      } else {
        console.error("Error fetching transactions:", axiosError.message);
      }
      return [];
    }
  }

  private transformTransactions(transactions: Transaction[]): Transaction[] {
    return transactions.map((transaction) => {
      const amount = parseFloat(transaction.amount);
      return {
        date: transaction.date,
        amount: amount.toString(),
        description: transaction.description,
      };
    });
  }

  private async loadTransactions(transactions: Transaction[]): Promise<void> {
    // we could use worker thread / child process if we receive large set of data to write it in CSV
    // for now I choose a simple sollution
    await csvWriter.writeRecords(transactions);
    console.log("CSV file created:", OUTPUT_FILE);
  }

  public async run(): Promise<void> {
    try {
      const transactions = await this.fetchTransactions();

      if (transactions.length === 0) {
        console.log("No transactions to process.");
        return;
      }
      const transformedTransactions = this.transformTransactions(transactions);
      await this.loadTransactions(transformedTransactions);
    } catch (error) {
      const axiosError = error as AxiosError;
      console.error("Error during ETL process:", axiosError.message);
    }
  }
}
