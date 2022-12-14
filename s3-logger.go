/*
 * Module folder name: s3-logger
 * Created Date: Tue Mar 15 2022
 * Author: Ashwin Rao (arao@okkular.io)
 * -----
 * Last Modified: Tue Apr 12 2022
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
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type S3LoggerAPI interface {
	Logw(keysAndValues ...interface{})
}

type s3Logger struct {
	log    *zap.Logger
	bucket string
	key    string
	cfg    aws.Config
}

func (l *s3Logger) Logw(keysAndValues ...interface{}) {
	log.Printf("In S3LoggerAPI.Logw")
	l.log.Sugar().Infow("", keysAndValues...)
}

func New(bucket, key string, cfg aws.Config) S3LoggerAPI {
	s3Client := s3.NewFromConfig(cfg)
	wrapper := zapWrapper{
		buf:      bytes.Buffer{},
		s3Client: s3Client,
		bucket:   bucket,
		key:      key,
	}

	s3loggerWriter := zapcore.AddSync(&wrapper)
	s3Encoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		TimeKey: "time",
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.UTC().Format("2006-01-02T15:04:05Z0700"))
			// 2019-08-13T04:39:11Z
		},
	})

	core := zapcore.NewCore(s3Encoder, s3loggerWriter, zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		return l < zapcore.ErrorLevel
	}))

	wLog := zap.New(core)
	defer wLog.Sync()

	logger := &s3Logger{
		log:    wLog,
		bucket: bucket,
		key:    key,
		cfg:    cfg,
	}
	return logger
}
