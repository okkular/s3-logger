/*
 * Module folder name: s3-logger
 * Created Date: Wed Mar 16 2022
 * Author: Ashwin Rao (arao@okkular.io)
 * -----
 * Last Modified: Sun Apr 10 2022
 * Modified By: Ashwin Rao
 * -----
 * MIT License
 *
 * Copyright (c) 2022 Okkular
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * -----
 * HISTORY:
 * Date      	By	Comments
 * ----------	---	---------------------------------------------------------
 */
package s3logger

import (
	"bytes"
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type zapWrapper struct {
	buf      bytes.Buffer
	s3Client *s3.Client
	bucket   string
	key      string
}

func (l *zapWrapper) Write(p []byte) (n int, err error) {
	log.Printf("Write called:\n%s", string(p))
	n, err = l.buf.Write(p)
	defer l.Sync()

	return n, err
}

func putData(client *s3.Client, bucket, key string, data []byte) error {
	putOutput, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	log.Printf("PutObject response; Output: %+v; Error: %s", putOutput, err)
	return err
}

func (l *zapWrapper) Sync() error {
	log.Printf("sync to S3 called; Bucket: %s; Key: %s", l.bucket, l.key)
	data := l.buf.Bytes()
	log.Printf("sync to S3 called: data received:\n%s", string(data))

	var err error
	if len(data) > 0 {
		err = putData(l.s3Client, l.bucket, l.key, data)
	}
	return err
}
