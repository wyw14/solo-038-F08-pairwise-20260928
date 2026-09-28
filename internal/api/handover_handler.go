package api

import (
	"net/http"
)

type handoverRequest struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type handoverAcceptRequest struct {
	Operator string `json:"operator"`
}

func (handler *Handler) requestSpliceHandover(writer http.ResponseWriter, request *http.Request) {
	var input handoverRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	handover, err := handler.runtime.RequestSpliceHandover(input.From, input.To)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, handover)
}

func (handler *Handler) acceptSpliceHandover(writer http.ResponseWriter, request *http.Request) {
	var input handoverAcceptRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	handover, err := handler.runtime.AcceptSpliceHandover(input.Operator)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusOK, handover)
}

func (handler *Handler) spliceHandovers(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, handler.runtime.SpliceHandovers())
}

func (handler *Handler) expireSpliceHandovers(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"revoked": handler.runtime.ExpireSpliceHandovers()})
}
