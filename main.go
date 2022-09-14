package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

/*
 * Complete the 'getUserTransaction' function below.
 *
 * The function is expected to return an INTEGER_ARRAY.
 * The function accepts following parameters:
 *  1. INTEGER uid
 *  2. STRING txnType
 *  3. STRING monthYear
 *
 *  https://jsonmock.hackerrank.com/api/transactions/search?userId=
 */

const endpoint = "https://jsonmock.hackerrank.com/api/transactions/search?userId="
const pageParam = "&page="
const credit = "credit"
const debit = "debit"

// UserTransactionResponse JsonResponse
type UserTransactionResponse struct {
	Page       int                           `json:"page"`
	PerPage    int                           `json:"per_page"`
	Total      int                           `json:"total"`
	TotalPages int                           `json:"total_pages"`
	Data       []UserTransactionDataResponse `json:"data"`
}

// UserTransactionDataResponse JsonResponse
type UserTransactionDataResponse struct {
	Id        int32                               `json:"id"`
	UserId    int                                 `json:"userId"`
	UserName  string                              `json:"userName"`
	Timestamp int64                               `json:"timestamp"`
	TxnType   string                              `json:"txnType"`
	Amount    string                              `json:"amount"`
	Location  UserTransactionLocationDataResponse `json:"location"`
}

// UserTransactionLocationDataResponse JsonResponse
type UserTransactionLocationDataResponse struct {
	Id      int    `json:"id"`
	Address string `json:"address"`
	City    string `json:"city"`
	ZipCode int    `json:"zipCode"`
}

// User Transaction Retriever
type userTransactionRetriever struct {
	apiEndpoint string
	uid         int32
	txnType     string
	monthYear   string
	curPage     int
	cache       *UserTransactionResponse
}

// Create a userTransactionRetriever for iterating to next page
func initializeUserTransactionRetriever(apiEndpoint string, uid int32, txnType string, monthYear string) *userTransactionRetriever {
	userTransactionRetriever := &userTransactionRetriever{apiEndpoint: apiEndpoint, uid: uid, txnType: txnType, monthYear: monthYear}

	userTransactionRetriever.curPage = 0
	userTransactionRetriever.cache = userTransactionRetriever.getRequest(nil)

	return userTransactionRetriever
}

func (u *userTransactionRetriever) hasNext() bool {
	return u.cache != nil && u.curPage < u.cache.TotalPages
}

func (u *userTransactionRetriever) getNext() *UserTransactionResponse {
	if !u.hasNext() {
		return nil
	}

	u.curPage += 1

	if u.curPage != u.cache.Page {
		u.cache = u.getRequest(&u.curPage)
	}

	return u.cache
}

func (u *userTransactionRetriever) getRequest(page *int) *UserTransactionResponse {
	requestEndpoint := endpoint + strconv.Itoa(int(u.uid))

	if page != nil {
		requestEndpoint += pageParam + strconv.Itoa(*page)
	}

	res, err := http.Get(requestEndpoint)
	checkError(err)

	resBody, err := ioutil.ReadAll(res.Body)
	checkError(err)

	var userTransactionResponse UserTransactionResponse

	err = json.Unmarshal(resBody, &userTransactionResponse)
	checkError(err)

	return &userTransactionResponse
}

func getUserTransaction(uid int32, txnType string, monthYear string) []int32 {
	// Check valid txnType
	txnType = strings.ToLower(txnType)
	if txnType != credit && txnType != debit {
		panic("Invalid txnType: " + txnType)
	}

	// Convert month year to int
	requestedTimeMonth, requestedTimeYear := convertMonthYearToInt(monthYear)

	// Get API retriever
	retriever := initializeUserTransactionRetriever(endpoint, uid, txnType, monthYear)

	var countDebitTxn int
	var sumDebitTxn float64
	var requestedTransactions []UserTransactionDataResponse

	// Loop through the API
	for retriever.hasNext() {
		response := retriever.getNext()

		// Loop through each page's data
		for _, v := range response.Data {
			// Convert milliseconds timestamp to Time, hackerRank Go compiler doesn't support UnixMilli(...)
			transactionTime := time.Unix(0, v.Timestamp*1_000_000)

			// Only take transactions with requested date
			if transactionTime.Month() == time.Month(requestedTimeMonth) && transactionTime.Year() == requestedTimeYear {
				// Sum and count the transactions of type 'debit'
				if strings.ToLower(v.TxnType) == debit {
					countDebitTxn += 1
					sumDebitTxn += convertAmountToFloat(v.Amount)
				}

				// Store transactions with requested transaction type
				if strings.ToLower(v.TxnType) == txnType {
					requestedTransactions = append(requestedTransactions, v)
				}
			}
		}
	}

	// Calculate average monthly spending
	average := sumDebitTxn / float64(countDebitTxn)

	// Return -1 if no matching requested transactions
	if len(requestedTransactions) == 0 {
		return []int32{-1}
	}

	// Find requested transactions with amount greater than the monthly average spending
	answers := make([]int32, 0)
	for _, v := range requestedTransactions {
		if convertAmountToFloat(v.Amount) > average {
			answers = append(answers, v.Id)
		}
	}

	// If still no matching transactions found, return -1
	if len(answers) == 0 {
		return []int32{-1}
	}

	return answers
}

func convertAmountToFloat(amount string) float64 {
	amount = strings.ReplaceAll(amount, ",", "")
	curAmount, err := strconv.ParseFloat(amount[1:], 64)
	checkError(err)

	return curAmount
}

func convertMonthYearToInt(monthYear string) (int, int) {
	requestedTimeStr := strings.Split(monthYear, "-")

	requestedTimeMonth, err := strconv.Atoi(requestedTimeStr[0])
	checkError(err)

	requestedTimeYear, err := strconv.Atoi(requestedTimeStr[1])
	checkError(err)

	return requestedTimeMonth, requestedTimeYear
}

// Below is the default code from HackerRank
func main() {
	reader := bufio.NewReaderSize(os.Stdin, 16*1024*1024)

	//stdout, err := os.Create(os.Getenv("OUTPUT_PATH"))
	//checkError(err)
	//
	//defer stdout.Close()
	//
	//writer := bufio.NewWriterSize(stdout, 16 * 1024 * 1024)

	uidTemp, err := strconv.ParseInt(strings.TrimSpace(readLine(reader)), 10, 64)
	checkError(err)
	uid := int32(uidTemp)

	txnType := readLine(reader)

	monthYear := readLine(reader)

	result := getUserTransaction(uid, txnType, monthYear)
	fmt.Println(result)
	//for i, resultItem := range result {
	//	fmt.Fprintf(writer, "%d", resultItem)
	//
	//	if i != len(result) - 1 {
	//		fmt.Fprintf(writer, "\n")
	//	}
	//}
	//
	//fmt.Fprintf(writer, "\n")
	//
	//writer.Flush()
}

func readLine(reader *bufio.Reader) string {
	str, _, err := reader.ReadLine()
	if err == io.EOF {
		return ""
	}

	return strings.TrimRight(string(str), "\r\n")
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
