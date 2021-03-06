/**
 * Copyright 2011-2012 @WalmartLabs, a division of Wal-Mart Stores, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.walmartlabs.mupd8.compression;

import org.xerial.snappy.Snappy;

import com.walmartlabs.mupd8.Constants;

public class SnappyCompression implements CompressionService {

	@Override
	public byte[] compress(byte[] uncompressed) throws Exception {
		assert(uncompressed.length < Constants.SLATE_CAPACITY);
		return Snappy.compress(uncompressed);
	}
	
	@Override
	public byte[] uncompress(byte[] compressed) throws Exception {
		return Snappy.uncompress(compressed);
	}
}
