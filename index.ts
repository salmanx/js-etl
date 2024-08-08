import TransactionETL from "./transactionEtl";

// Get command-line arguments
const args = process.argv.slice(2);

if (args.length !== 2) {
  console.error(
    "Please provide exactly two date arguments in YYYY-MM-DD format."
  );
  process.exit(1);
}

const [fromDate, toDate] = args;

const etl = new TransactionETL(fromDate, toDate);
etl.run();
