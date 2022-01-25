package recovery

/**
 * Read log file from disk, redo and undo.
 */
 class LogRecovery {
	public:
	 LogRecovery(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager)
		 : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), offset_(0) {
	   log_buffer_ = new char[LOG_BUFFER_SIZE];
	 }
   
	 ~LogRecovery() {
	   delete[] log_buffer_;
	   log_buffer_ = nullptr;
	 }
   
	 void Redo();
	 void Undo();
	 bool DeserializeLogRecord(const char *data, LogRecord *log_record);
   
	private:
	 DiskManager *disk_manager_ __attribute__((__unused__));
	 BufferPoolManager *buffer_pool_manager_ __attribute__((__unused__));
   
	 /** Maintain active transactions and its corresponding latest lsn. */
	 std::unordered_map<txn_id_t, lsn_t> active_txn_;
	 /** Mapping the log sequence number to log file offset for undos. */
	 std::unordered_map<lsn_t, int> lsn_mapping_;
   
	 int offset_ __attribute__((__unused__));
	 char *log_buffer_;
};

/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
 bool LogRecovery::DeserializeLogRecord(const char *data, LogRecord *log_record) {
	if (LOG_BUFFER_SIZE - (data - log_buffer_) < LogRecord::HEADER_SIZE) {
	  return false;
	}
	// First, unserialize the must have fields(20 bytes in total)
	memcpy(log_record, data, LogRecord::HEADER_SIZE);
	if (log_record->size_ <= 0) {
	  return false;
	}
	if (LOG_BUFFER_SIZE - (data - log_buffer_) < log_record->size_) {
	  return false;
	}
	int pos = LogRecord::HEADER_SIZE;
	if (log_record->log_record_type_ == LogRecordType::INSERT) {
	  memcpy(&log_record->insert_rid_, data + pos, sizeof(RID));
	  pos += sizeof(RID);
	  // we have provided serialize function for tuple class
	  log_record->insert_tuple_.DeserializeFrom(data + pos);
	} else if (log_record->log_record_type_ == LogRecordType::APPLYDELETE ||
			   log_record->log_record_type_ == LogRecordType::MARKDELETE ||
			   log_record->log_record_type_ == LogRecordType::ROLLBACKDELETE) {
	  memcpy(&log_record->delete_rid_, data + pos, sizeof(RID));
	  pos += sizeof(RID);
	  // we have provided serialize function for tuple class
	  log_record->delete_tuple_.DeserializeFrom(data + pos);
	} else if (log_record->log_record_type_ == LogRecordType::UPDATE) {
	  memcpy(&log_record->update_rid_, data + pos, sizeof(RID));
	  pos += sizeof(RID);
	  // we have provided serialize function for tuple class
	  log_record->old_tuple_.DeserializeFrom(data + pos);
	  pos += sizeof(log_record->old_tuple_.GetLength() + sizeof(uint32_t));
	  log_record->new_tuple_.DeserializeFrom(data + pos);
	} else if (log_record->log_record_type_ == LogRecordType::NEWPAGE) {
	  memcpy(&log_record->prev_page_id_, data + pos, sizeof(page_id_t));
	}
  
	return true;
  }
  
  /*
   *redo phase on TABLE PAGE level(table/table_page.h)
   *read log file from the beginning to end (you must prefetch log records into
   *log buffer to reduce unnecessary I/O operations), remember to compare page's
   *LSN with log_record's sequence number, and also build active_txn_ table &
   *lsn_mapping_ table
   */
  void LogRecovery::Redo() {
	int file_offset = 0;
	while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, file_offset)) {
	  int buffer_offset = 0;
	  LogRecord log_record;
	  while (DeserializeLogRecord(log_buffer_ + buffer_offset, &log_record)) {
		active_txn_[log_record.txn_id_] = log_record.lsn_;
		lsn_mapping_[log_record.lsn_] = file_offset + buffer_offset;
		if (log_record.log_record_type_ == LogRecordType::INSERT) {
		  auto page =
			  static_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.insert_rid_.GetPageId(), nullptr));
		  if (page->GetLSN() < log_record.GetLSN()) {
			page->InsertTuple(log_record.insert_tuple_, &log_record.insert_rid_, nullptr, nullptr, nullptr);
			page->SetLSN(log_record.GetLSN());
		  }
		  buffer_pool_manager_->UnpinPage(log_record.insert_rid_.GetPageId(), true, nullptr);
		} else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE) {
		  auto page =
			  static_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.delete_rid_.GetPageId(), nullptr));
		  if (page->GetLSN() < log_record.GetLSN()) {
			page->ApplyDelete(log_record.delete_rid_, nullptr, nullptr);
			page->SetLSN(log_record.GetLSN());
		  }
		  buffer_pool_manager_->UnpinPage(log_record.delete_rid_.GetPageId(), true, nullptr);
		} else if (log_record.log_record_type_ == LogRecordType::MARKDELETE) {
		  auto page =
			  static_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.delete_rid_.GetPageId(), nullptr));
		  if (page->GetLSN() < log_record.GetLSN()) {
			page->MarkDelete(log_record.delete_rid_, nullptr, nullptr, nullptr);
			page->SetLSN(log_record.GetLSN());
		  }
		  buffer_pool_manager_->UnpinPage(log_record.delete_rid_.GetPageId(), true, nullptr);
		} else if (log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
		  auto page =
			  static_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.delete_rid_.GetPageId(), nullptr));
		  if (page->GetLSN() < log_record.GetLSN()) {
			page->RollbackDelete(log_record.delete_rid_, nullptr, nullptr);
			page->SetLSN(log_record.GetLSN());
		  }
		  buffer_pool_manager_->UnpinPage(log_record.delete_rid_.GetPageId(), true, nullptr);
		} else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
		  auto page =
			  static_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.update_rid_.GetPageId(), nullptr));
		  if (page->GetLSN() < log_record.GetLSN()) {
			page->UpdateTuple(log_record.new_tuple_, &log_record.old_tuple_, log_record.update_rid_, nullptr, nullptr,
							  nullptr);
			page->SetLSN(log_record.GetLSN());
		  }
		  buffer_pool_manager_->UnpinPage(log_record.update_rid_.GetPageId(), true, nullptr);
		} else if (log_record.log_record_type_ == LogRecordType::BEGIN) {
		  active_txn_[log_record.txn_id_] = log_record.lsn_;
		} else if (log_record.log_record_type_ == LogRecordType::COMMIT) {
		  active_txn_.erase(log_record.txn_id_);
		} else if (log_record.log_record_type_ == LogRecordType::NEWPAGE) {
		  page_id_t page_id;
		  auto new_page = static_cast<TablePage *>(buffer_pool_manager_->NewPage(&page_id, nullptr));
		  LOG_DEBUG("page_id: %d", page_id);
		  new_page->Init(page_id, PAGE_SIZE, log_record.prev_page_id_, nullptr, nullptr);
		  buffer_pool_manager_->UnpinPage(page_id, true, nullptr);
		}
		buffer_offset += log_record.size_;
	  }
	  // incomplete log record
	  file_offset += buffer_offset;
	}
  }
  
  /*
   *undo phase on TABLE PAGE level(table/table_page.h)
   *iterate through active txn map and undo each operation
   */
  void LogRecovery::Undo() {
	LogRecord log_record;
	for (auto it : active_txn_) {
	  auto lsn = it.second;
	  while (lsn != INVALID_LSN) {
		auto file_offset = lsn_mapping_[lsn];
		LOG_DEBUG("file_offset: %d", file_offset);
		disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, file_offset);
		DeserializeLogRecord(log_buffer_, &log_record);
		if (log_record.log_record_type_ == LogRecordType::INSERT) {
		  auto page =
			  static_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.insert_rid_.GetPageId(), nullptr));
		  LOG_DEBUG("insert log type, page lsn:%d, log lsn:%d", page->GetLSN(), log_record.GetLSN());
		  page->ApplyDelete(log_record.insert_rid_, nullptr, nullptr);
		  buffer_pool_manager_->UnpinPage(log_record.insert_rid_.GetPageId(), true, nullptr);
		} else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE) {
		  auto page =
			  static_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.delete_rid_.GetPageId(), nullptr));
		  page->InsertTuple(log_record.delete_tuple_, &log_record.delete_rid_, nullptr, nullptr, nullptr);
		  buffer_pool_manager_->UnpinPage(log_record.delete_rid_.GetPageId(), true, nullptr);
		} else if (log_record.log_record_type_ == LogRecordType::MARKDELETE) {
		  auto page =
			  static_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.delete_rid_.GetPageId(), nullptr));
		  page->RollbackDelete(log_record.delete_rid_, nullptr, nullptr);
		  buffer_pool_manager_->UnpinPage(log_record.delete_rid_.GetPageId(), true, nullptr);
		} else if (log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
		  auto page =
			  static_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.delete_rid_.GetPageId(), nullptr));
		  page->MarkDelete(log_record.delete_rid_, nullptr, nullptr, nullptr);
		  buffer_pool_manager_->UnpinPage(log_record.delete_rid_.GetPageId(), true, nullptr);
		} else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
		  auto page =
			  static_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.update_rid_.GetPageId(), nullptr));
		  page->UpdateTuple(log_record.old_tuple_, &log_record.new_tuple_, log_record.update_rid_, nullptr, nullptr,
							nullptr);
		  buffer_pool_manager_->UnpinPage(log_record.update_rid_.GetPageId(), true, nullptr);
		}
		lsn = log_record.prev_lsn_;
	  }
	}
	buffer_pool_manager_->FlushAllPages();
  }