package api

import (
	"net/http"
	"time"
)

type handoverRequest struct {
	ToOwner    string `json:"to_owner"`
	TTLSeconds int    `json:"ttl_seconds"`
}

type handoverAcceptRequest struct {
	Owner string `json:"owner"`
}

func (handler *Handler) spliceHandovers(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, handler.runtime.SpliceHandovers())
}

func (handler *Handler) requestSpliceHandover(writer http.ResponseWriter, request *http.Request) {
	var input handoverRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	handover, err := handler.runtime.RequestSpliceHandover(input.ToOwner, time.Duration(input.TTLSeconds)*time.Second)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusCreated, handover)
}

func (handler *Handler) acceptSpliceHandover(writer http.ResponseWriter, request *http.Request) {
	var input handoverAcceptRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	handover, err := handler.runtime.AcceptSpliceHandover(input.Owner)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusOK, handover)
}

func (handler *Handler) withdrawSpliceHandover(writer http.ResponseWriter, request *http.Request) {
	handover, err := handler.runtime.WithdrawSpliceHandover()
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusOK, handover)
}
